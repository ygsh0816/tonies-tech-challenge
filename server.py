import asyncio
import struct
import logmessage_pb2


async def handle_client(reader, writer):
    try:
        while True:
            # Read the length of the next message
            data = await reader.readexactly(8)
            length = struct.unpack('!Q', data)[0]

            # Read the protobuf message
            data = await reader.readexactly(length)

            # Deserialize the protobuf message
            log_message = logmessage_pb2.LogMessage()
            log_message.ParseFromString(data)

            # Format and print the log message
            formatted_message = (
                f"Log Level: {log_message.log_level}, "
                f"Logger: {log_message.logger}, "
                f"MAC: {log_message.mac.hex()}, "
                f"Message: {log_message.message}"
            )
            print(formatted_message)
    except asyncio.IncompleteReadError:
        pass  # Handle client disconnects gracefully
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    server = await asyncio.start_server(
        handle_client, '0.0.0.0', 8888)

    async with server:
        await server.serve_forever()


asyncio.run(main())
