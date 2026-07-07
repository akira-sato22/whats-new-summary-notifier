import * as fs from 'fs';
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { WhatsNewSummaryNotifierStack } from '../lib/whats-new-summary-notifier-stack';

// cdk.json の実際の context を使って合成する。
// PythonFunction の Docker バンドリングはテストでは不要なためスキップする。
function synthTemplate(): Template {
  const cdkJson = JSON.parse(fs.readFileSync(path.join(__dirname, '../cdk.json'), 'utf8'));
  const app = new cdk.App({
    context: {
      ...cdkJson.context,
      'aws:cdk:bundling-stacks': [],
    },
  });
  const stack = new WhatsNewSummaryNotifierStack(app, 'TestStack', {
    env: { account: '123456789012', region: 'us-east-1' },
  });
  return Template.fromStack(stack);
}

describe('WhatsNewSummaryNotifierStack', () => {
  const template = synthTemplate();

  test('RSS履歴テーブルがストリームとTTL付きで作成される', () => {
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      TableName: 'AWSUpdatesRSSHistory',
      KeySchema: [
        { AttributeName: 'url', KeyType: 'HASH' },
        { AttributeName: 'notifier_name', KeyType: 'RANGE' },
      ],
      BillingMode: 'PAY_PER_REQUEST',
      StreamSpecification: { StreamViewType: 'NEW_AND_OLD_IMAGES' },
      TimeToLiveSpecification: { AttributeName: 'ttl', Enabled: true },
    });
  });

  test('3つのLambda関数が定義される', () => {
    for (const functionName of [
      'WhatsNewSummary-Notifier',
      'WhatsNewSummary-Crawler',
      'WhatsNewSummary-MarkdownGenerator',
    ]) {
      template.hasResourceProperties('AWS::Lambda::Function', {
        FunctionName: functionName,
        Runtime: 'python3.11',
      });
    }
  });

  test('通知LambdaはDynamoDBストリームをバッチサイズ1で処理する', () => {
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 1,
      StartingPosition: 'LATEST',
    });
  });

  test('クローラーは30分ごとにスケジュールされる', () => {
    template.hasResourceProperties('AWS::Events::Rule', {
      ScheduleExpression: 'cron(*/30 * * * ? *)',
      State: 'ENABLED',
    });
  });

  test('週間サマリーは毎週月曜8:00 UTCにスケジュールされる', () => {
    template.hasResourceProperties('AWS::Events::Rule', {
      ScheduleExpression: 'cron(0 8 ? * 1 *)',
      State: 'ENABLED',
    });
  });

  test('Markdown生成LambdaにSSMパラメータ名が環境変数で渡される', () => {
    template.hasResourceProperties('AWS::Lambda::Function', {
      FunctionName: 'WhatsNewSummary-MarkdownGenerator',
      Environment: {
        Variables: Match.objectLike({
          SLACK_BOT_TOKEN_PARAMETER: '/WhatsNew/SLACK_BOT_TOKEN',
          SLACK_CHANNEL_ID_PARAMETER: '/WhatsNew/SLACK_CHANNEL_ID',
        }),
      },
    });
  });

  test('Bedrock権限はワイルドカードリソースに付与されない', () => {
    const policies = template.findResources('AWS::IAM::Policy');
    for (const policy of Object.values(policies)) {
      const statements = policy.Properties.PolicyDocument.Statement as Array<{
        Action: string | string[];
        Resource: unknown;
      }>;
      for (const statement of statements) {
        const actions = Array.isArray(statement.Action) ? statement.Action : [statement.Action];
        if (actions.includes('bedrock:InvokeModel')) {
          const resources = Array.isArray(statement.Resource)
            ? statement.Resource
            : [statement.Resource];
          expect(resources).not.toContain('*');
        }
      }
    }
  });
});
