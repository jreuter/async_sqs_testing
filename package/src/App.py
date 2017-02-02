import sys
import asyncio
import aiobotocore
import logging
from botocore.exceptions import ClientError
from random import randint

logging.basicConfig( stream=sys.stdout, level=logging.INFO)


async def coro1():
    res = randint(0, 7)
    await asyncio.sleep(res)
    logging.info('coro1 finished with output {}'.format(res))
    return res

async def coro2():
    res = await coro1()
    res = res * res
    await asyncio.sleep(60 * res)
    logging.info('coro2 finished with output {}'.format(res))
    return res

async def coro3(loop):
    queue_name = 'jreuter-edge-detection'
    logging.info('C3 - About to call SQS')
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client('sqs', region_name='us-east-1') as client:
        try:
            queue = await client.get_queue_url(QueueName=queue_name)
        except ClientError as e:
            if e.response['Error']['Code'] == "AWS.SimpleQueueService.NonExistentQueue":
                logging.info(e.response['Error']['Message'])
                logging.info("Creating SQS queue %s in region %s...", queue_name, 'us-east-1')
                queue = await client.create_queue(QueueName=queue_name)
            else:
                raise
        queue_url = queue.get('QueueUrl')
        logging.info("C3 - recieving messages")
        while True:
            result = await client.receive_message(WaitTimeSeconds=20,
                                                  MaxNumberOfMessages=1,
                                                  QueueUrl=queue_url)

            if 'Messages' in result:
                logging.info('C3 - We got some messages')
                for message in result['Messages']:
                    logging.info(message['Body'])
                    await client.delete_message(ReceiptHandle=str(message.get('ReceiptHandle')),
                                                QueueUrl=queue_url)
            else:
                logging.info('C3 - We got no messages')
    return

async def main(loop):
    await asyncio.gather(
        coro3(loop),
        coro2(),
        coro2()
    )


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
