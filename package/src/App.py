import aiobotocore
import asyncio
import logging
from botocore.exceptions import ClientError
from random import randint

async def coro1():
    res = randint(0, 7)
    await asyncio.sleep(res)
    logging.error('coro1 finished with output {}'.format(res))
    return res

async def coro2():
    res = await coro1()
    res = res * res
    await asyncio.sleep(res)
    logging.error('coro2 finished with output {}'.format(res))
    return res

async def coro3(loop):
    queue_name = 'jreuter-edge-detection'
    logging.error('About to call SQS')
    session = aiobotocore.get_session(loop=loop)
    client = session.create_client('sqs', region_name='us-east-1')
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

    result = await client.receive_message(WaitTimeSeconds=20,
                                          MaxNumberOfMessages=1,
                                          QueueUrl=queue_url)

    if 'Messages' in result:
        logging.info('We got some messages')
    else:
        logging.info('We got no messages')
    return

async def main(loop):
    await asyncio.gather(
        coro3(loop),
        coro2(),
        coro2(),
        coro3(loop)
    )


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
