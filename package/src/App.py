import sys
import asyncio
import aiobotocore
import logging
from aiohttp import web
from botocore.exceptions import ClientError
from random import randint

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class co1:
    @staticmethod
    async def coro1():
        res = randint(0, 7)
        await asyncio.sleep(res)
        logging.info('coro1 finished with output {}'.format(res))
        return res


class co2:
    @staticmethod
    async def coro2(app):
        res = await co1.coro1()
        res = res * res
        await asyncio.sleep(res)
        logging.info('coro2 finished with output {}'.format(res))
        return res

class TestHandler(web.View):

     async def get(self):
         logging.info("hit web endpoint")
         return web.json_response({"Status": "Ok"})


async def coro3(app):
    queue_name = 'jreuter-edge-detection'
    logging.info('C3 - About to call SQS')

    loop = asyncio.get_event_loop()
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
            await asyncio.sleep(60)

def make_app():
    app = web.Application(loop=loop)
    app.on_startup.append(start_sqs_task)
    app.on_startup.append(co2.coro2)
    app.on_cleanup.append(cleanup_sqs_task)

    app.router.add_route('*',
                         r'/{base:(places)?\/?}',
                         TestHandler)

    return app

async def start_sqs_task(app):
     app['sqs_listener'] = app.loop.create_task(coro3(app))

async def cleanup_sqs_task(app):
     app['sqs_listener'].cancel()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = make_app()
    web.run_app(app, host='0.0.0.0', port=5000)
