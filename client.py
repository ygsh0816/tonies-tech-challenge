import asyncio
import struct
import logmessage_pb2


async def send_log_message(message, host='127.0.0.1', port=8888):
    reader, writer = await asyncio.open_connection(host, port)

    # Serialize the protobuf message
    serialized_message = message.SerializeToString()
    message_length = len(serialized_message)

    # Prefix the message with its length
    packed_length = struct.pack('!Q', message_length)

    # Send the length-prefixed message
    writer.write(packed_length + serialized_message)
    await writer.drain()

    print(f"Sent: {message}")

    writer.close()
    await writer.wait_closed()


def create_log_message(log_level, logger, mac, message=None):
    log_message = logmessage_pb2.LogMessage()
    log_message.log_level = log_level
    log_message.logger = logger
    log_message.mac = mac
    if message:
        log_message.message = message
    return log_message


async def main():
    # Create a list of tasks to send 100 concurrent messages
    tasks = []
    for i in range(100):
        test_message = create_log_message(
            log_level="INFO",
            logger=f"TestLogger{i}",
            mac=b'\x00\x1A\x2B\x3C\x4D\x5E',
            message=f"This is test log message {i}."
        )
        tasks.append(send_log_message(test_message))

    # Run all tasks concurrently
    await asyncio.gather(*tasks)


asyncio.run(main())
