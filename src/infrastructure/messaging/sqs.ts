import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import type { AppConfig } from "../../config";

export interface QueueUrls {
  wager: string;
  dlq: string;
  events: string;
}

export function createSqsClient(config: AppConfig): SQSClient {
  return new SQSClient({
    endpoint: config.SQS_ENDPOINT,
    region: config.AWS_REGION,
    credentials: { accessKeyId: config.AWS_ACCESS_KEY_ID, secretAccessKey: config.AWS_SECRET_ACCESS_KEY },
    maxAttempts: 3,
  });
}

/**
 * Resolves (and, when allowed, creates) the queues. CreateQueue is idempotent for identical
 * attributes, so every instance can run it at boot. The redrive policy is a backstop: the consumer
 * dead-letters explicitly before reaching it.
 */
export async function resolveQueues(sqs: SQSClient, config: AppConfig): Promise<QueueUrls> {
  const fifo = { FifoQueue: "true" };
  const ensure = async (name: string, attributes: Record<string, string>) => {
    if (!config.SQS_AUTO_CREATE_QUEUES) {
      return (await sqs.send(new GetQueueUrlCommand({ QueueName: name }))).QueueUrl!;
    }
    return (await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attributes }))).QueueUrl!;
  };
  const dlq = await ensure(config.SQS_WAGER_DLQ, { ...fifo, MessageRetentionPeriod: "1209600" });
  const dlqArn = (
    await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq, AttributeNames: ["QueueArn"] }))
  ).Attributes?.QueueArn;
  const wager = await ensure(config.SQS_WAGER_QUEUE, {
    ...fifo,
    VisibilityTimeout: String(config.CONSUMER_VISIBILITY_TIMEOUT_S),
    ...(dlqArn
      ? { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: config.SQS_REDRIVE_MAX_RECEIVES }) }
      : {}),
  });
  // The events queue may be FIFO (ordering per aggregate + broker dedup) or standard.
  const events = await ensure(config.SQS_EVENTS_QUEUE, config.SQS_EVENTS_QUEUE.endsWith(".fifo") ? fifo : {});
  return { wager, dlq, events };
}
