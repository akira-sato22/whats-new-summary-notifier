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
import { BlockPublicAccess, Bucket } from 'aws-cdk-lib/aws-s3';
import * as path from 'path';

interface SummarizerConfig {
  outputLanguage: string;
  persona: string;
}

interface NotifierConfig {
  destination: 'slack' | 'teams';
  summarizerName: string;
  webhookUrlParameterName: string;
  rssUrl: Record<string, string>;
  schedule?: CronOptions;
}

const SLACK_BOT_TOKEN_PARAMETER_NAME = '/WhatsNew/SLACK_BOT_TOKEN';
const SLACK_CHANNEL_ID_PARAMETER_NAME = '/WhatsNew/SLACK_CHANNEL_ID';

export class WhatsNewSummaryNotifierStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const region = cdk.Stack.of(this).region;
    const accountId = cdk.Stack.of(this).account;

    const modelRegion: string = this.node.tryGetContext('modelRegion');
    const modelId: string = this.node.tryGetContext('modelId');

    const notifiers: Record<string, NotifierConfig> = this.node.tryGetContext('notifiers');
    const summarizers: Record<string, SummarizerConfig> = this.node.tryGetContext('summarizers');

    // Lambda 用ロールを作成し、CloudWatch Logs への書き込み権限と追加ポリシーを付与する
    const createLambdaRole = (
      roleId: string,
      policyId: string,
      extraStatements: PolicyStatement[] = []
    ): Role => {
      const role = new Role(this, roleId, {
        assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      });
      role.attachInlinePolicy(
        new Policy(this, policyId, {
          statements: [
            new PolicyStatement({
              actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
              effect: Effect.ALLOW,
              resources: [`arn:aws:logs:${region}:${accountId}:log-group:*`],
            }),
            ...extraStatements,
          ],
        })
      );
      return role;
    };

    // Role for Lambda Function to post new entries written to DynamoDB to Slack or Microsoft Teams
    const notifyNewEntryRole = createLambdaRole(
      'NotifyNewEntryRole',
      'AllowNotifyNewEntryLogging',
      [
        new PolicyStatement({
          actions: ['bedrock:InvokeModel'],
          effect: Effect.ALLOW,
          resources: [
            'arn:aws:bedrock:*::foundation-model/*',
            `arn:aws:bedrock:*:${accountId}:inference-profile/*`,
          ],
        }),
      ]
    );

    // Role for Lambda function to fetch RSS and write to DynamoDB
    const newsCrawlerRole = createLambdaRole('NewsCrawlerRole', 'AllowNewsCrawlerLogging');

    // Role for Lambda function to generate weekly summary markdown
    const markdownGeneratorRole = createLambdaRole(
      'MarkdownGeneratorRole',
      'AllowMarkdownGeneratorLogging',
      [
        new PolicyStatement({
          actions: ['ssm:GetParameter'],
          effect: Effect.ALLOW,
          resources: [
            `arn:aws:ssm:${region}:${accountId}:parameter${SLACK_BOT_TOKEN_PARAMETER_NAME}`,
            `arn:aws:ssm:${region}:${accountId}:parameter${SLACK_CHANNEL_ID_PARAMETER_NAME}`,
          ],
        }),
      ]
    );

    // S3バケットを作成（週間サマリーMarkdownの保存用）
    const summaryBucket = new Bucket(this, 'WeeklySummaryBucket', {
      bucketName: `aws-whats-new-weekly-summary-${accountId}-${region}`,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    summaryBucket.grantWrite(markdownGeneratorRole);

    // DynamoDB to store RSS data
    const rssHistoryTable = new Table(this, 'WhatsNewRSSHistory', {
      tableName: 'AWSUpdatesRSSHistory',
      partitionKey: { name: 'url', type: AttributeType.STRING },
      sortKey: { name: 'notifier_name', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      stream: StreamViewType.NEW_AND_OLD_IMAGES,
      timeToLiveAttribute: 'ttl',
    });

    // Allow the crawler to read and write RSS history
    rssHistoryTable.grantReadWriteData(newsCrawlerRole);
    // Allow the notify-to-app Lambda to update items with summaries
    rssHistoryTable.grantWriteData(notifyNewEntryRole);
    // Allow the markdown generator to read RSS history
    rssHistoryTable.grantReadData(markdownGeneratorRole);

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
        DDB_TABLE_NAME: rssHistoryTable.tableName,
      },
    });
    notifyNewEntry.addEventSource(
      new DynamoEventSource(rssHistoryTable, {
        startingPosition: StartingPosition.LATEST,
        batchSize: 1,
      })
    );

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

    // Markdown生成Lambda関数
    const markdownGenerator = new PythonFunction(this, 'MarkdownGenerator', {
      functionName: 'WhatsNewSummary-MarkdownGenerator',
      runtime: Runtime.PYTHON_3_11,
      entry: path.join(__dirname, '../lambda/markdown-generator'),
      handler: 'handler',
      index: 'index.py',
      timeout: cdk.Duration.seconds(180),
      logRetention: RetentionDays.TWO_WEEKS,
      role: markdownGeneratorRole,
      environment: {
        DDB_TABLE_NAME: rssHistoryTable.tableName,
        S3_BUCKET_NAME: summaryBucket.bucketName,
        SLACK_BOT_TOKEN_PARAMETER: SLACK_BOT_TOKEN_PARAMETER_NAME,
        SLACK_CHANNEL_ID_PARAMETER: SLACK_CHANNEL_ID_PARAMETER_NAME,
      },
    });

    // Slackトークン・チャンネルIDパラメータへの読み取り権限を追加
    StringParameter.fromSecureStringParameterAttributes(this, 'SlackBotTokenForMarkdownGenerator', {
      parameterName: SLACK_BOT_TOKEN_PARAMETER_NAME,
    }).grantRead(markdownGeneratorRole);
    StringParameter.fromSecureStringParameterAttributes(
      this,
      'SlackChannelIdForMarkdownGenerator',
      {
        parameterName: SLACK_CHANNEL_ID_PARAMETER_NAME,
      }
    ).grantRead(markdownGeneratorRole);

    // Markdown生成のスケジュールルールを設定（毎週月曜 8:00 UTC）
    const markdownGeneratorRule = new Rule(this, 'MarkdownGeneratorRule', {
      schedule: Schedule.cron({
        minute: '0',
        hour: '8',
        weekDay: '1',
      }),
      enabled: true,
    });

    markdownGeneratorRule.addTarget(
      new LambdaFunction(markdownGenerator, {
        event: RuleTargetInput.fromObject({ days: 7 }),
        retryAttempts: 2,
      })
    );

    for (const [notifierName, notifier] of Object.entries(notifiers)) {
      // Use the notifier's cron options if defined, otherwise run every 30 minutes
      const schedule: CronOptions = notifier.schedule || {
        minute: '*/30',
        hour: '*',
        day: '*',
        month: '*',
        year: '*',
      };
      const webhookUrlParameterStore = StringParameter.fromSecureStringParameterAttributes(
        this,
        `webhookUrlParameterStore-${notifierName}`,
        {
          parameterName: notifier.webhookUrlParameterName,
        }
      );

      // add permission to Lambda Role
      webhookUrlParameterStore.grantRead(notifyNewEntryRole);

      // Scheduled Rule for RSS Crawler
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
