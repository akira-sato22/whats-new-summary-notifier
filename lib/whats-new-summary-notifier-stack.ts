import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import { Table, AttributeType, BillingMode, StreamViewType } from 'aws-cdk-lib/aws-dynamodb';
import { Rule, Schedule, RuleTargetInput, CronOptions } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { Role, Policy, ServicePrincipal, PolicyStatement, Effect } from 'aws-cdk-lib/aws-iam';
import { Runtime, StartingPosition } from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';
import { RetentionDays } from 'aws-cdk-lib/aws-logs';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import * as path from 'path';

import { Tags } from './tags';

export class WhatsNewSummaryNotifierStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const region = cdk.Stack.of(this).region;
    const accountId = cdk.Stack.of(this).account;

    const modelRegion = this.node.tryGetContext('modelRegion');
    const modelId = this.node.tryGetContext('modelId');

    const notifiers: [] = this.node.tryGetContext('notifiers');
    const summarizers: [] = this.node.tryGetContext('summarizers');

    // Role for Lambda Function to post new entries written to DynamoDB to Slack or Microsoft Teams
    const notifyNewEntryRole = new Role(this, 'NotifyNewEntryRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
    });
    notifyNewEntryRole.attachInlinePolicy(
      new Policy(this, 'AllowNotifyNewEntryLogging', {
        statements: [
          new PolicyStatement({
            actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
            effect: Effect.ALLOW,
            resources: [`arn:aws:logs:${region}:${accountId}:log-group:*`],
          }),
          new PolicyStatement({
            actions: ['bedrock:InvokeModel'],
            effect: Effect.ALLOW,
            resources: ['*'],
          }),
        ],
      })
    );
    // cdk.Tags.of(notifyNewEntryRole).add(Tags.keys.purpose, Tags.values.purpose);

    // Role for Lambda function to fetch RSS and write to DynamoDB
    const newsCrawlerRole = new Role(this, 'NewsCrawlerRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
    });
    newsCrawlerRole.attachInlinePolicy(
      new Policy(this, 'AllowNewsCrawlerLogging', {
        statements: [
          new PolicyStatement({
            actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
            effect: Effect.ALLOW,
            resources: [`arn:aws:logs:${region}:${accountId}:log-group:*`],
          }),
        ],
      })
    );
    // cdk.Tags.of(newsCrawlerRole).add(Tags.keys.purpose, Tags.values.purpose);

    // DynamoDB to store RSS data
    const rssHistoryTable = new Table(this, 'WhatsNewRSSHistory', {
      tableName: 'AWSUpdatesRSSHistory',
      partitionKey: { name: 'url', type: AttributeType.STRING },
      sortKey: { name: 'notifier_name', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      stream: StreamViewType.NEW_AND_OLD_IMAGES,
      timeToLiveAttribute: 'ttl',
    });
    // cdk.Tags.of(rssHistoryTable).add(Tags.keys.purpose, Tags.values.purpose);

    // Lambda Function to post new entries written to DynamoDB to Slack or Microsoft Teams
    const notifyNewEntry = new PythonFunction(this, 'NotifyNewEntry', {
      functionName: 'WhatsNewSummary-Notifier',
      runtime: Runtime.PYTHON_3_11,
      entry: path.join(__dirname, '../lambda/notify-to-app'),
      handler: 'handler',
      index: 'index.py',
      timeout: cdk.Duration.seconds(180),
      logRetention: RetentionDays.TWO_WEEKS,
      role: notifyNewEntryRole,
      reservedConcurrentExecutions: 1,
      environment: {
        MODEL_ID: modelId,
        MODEL_REGION: modelRegion,
        NOTIFIERS: JSON.stringify(notifiers),
        SUMMARIZERS: JSON.stringify(summarizers),
      },
    });
    notifyNewEntry.addEventSource(
      new DynamoEventSource(rssHistoryTable, {
        startingPosition: StartingPosition.LATEST,
        batchSize: 1,
      })
    );
    // cdk.Tags.of(notifyNewEntry).add(Tags.keys.purpose, Tags.values.purpose);

    // Allow writing to DynamoDB
    rssHistoryTable.grantWriteData(newsCrawlerRole);

    // Lambda Function to fetch RSS and write to DynamoDB
    const newsCrawler = new PythonFunction(this, `newsCrawler`, {
      functionName: 'WhatsNewSummary-Crawler',
      runtime: Runtime.PYTHON_3_11,
      entry: path.join(__dirname, '../lambda/rss-crawler'),
      handler: 'handler',
      index: 'index.py',
      timeout: cdk.Duration.seconds(60),
      logRetention: RetentionDays.TWO_WEEKS,
      role: newsCrawlerRole,
      environment: {
        DDB_TABLE_NAME: rssHistoryTable.tableName,
        NOTIFIERS: JSON.stringify(notifiers),
      },
    });
    // cdk.Tags.of(newsCrawler).add(Tags.keys.purpose, Tags.values.purpose);

    for (const notifierName in notifiers) {
      const notifier = notifiers[notifierName];
      // const cron is a cronOption defined in a notifier. if it is not defined, set default schedule (every hour)
      const schedule: CronOptions = notifier['schedule'] || {
        minute: '0',
        hour: '*',
        day: '*',
        month: '*',
        year: '*',
      };
      const webhookUrlParameterName = notifier['webhookUrlParameterName'];
      const webhookUrlParameterStore = StringParameter.fromSecureStringParameterAttributes(
        this,
        `webhookUrlParameterStore-${notifierName}`,
        {
          parameterName: webhookUrlParameterName,
        }
      );

      // add permission to Lambda Role
      webhookUrlParameterStore.grantRead(notifyNewEntryRole);

      // Scheduled Rule for RSS Crawler
      // Run every hour, 24 hours a day
      // see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html#CronExpressions
      const rule = new Rule(this, `CheckUpdate-${notifierName}`, {
        schedule: Schedule.cron(schedule),
        enabled: true,
      });

      rule.addTarget(
        new LambdaFunction(newsCrawler, {
          event: RuleTargetInput.fromObject({ notifierName, notifier }),
          retryAttempts: 2,
        })
      );
    }
  }
}
