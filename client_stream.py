import cv2
import numpy as np
import grpc

import video_service_pb2 as pb2
import video_service_pb2_grpc as pb2_grpc


def main():
    channel = grpc.insecure_channel("localhost:50051")
    stub = pb2_grpc.VideoServiceStub(channel)

    # Listar videos
    video_list = stub.ListVideos(pb2.Empty())
    if not video_list.videos:
        print("[CLIENT] No hay videos en el servidor.")
        return

    print("[CLIENT] Videos disponibles:")
    for i, v in enumerate(video_list.videos):
        print(f"  [{i}] {v.filename} - {v.title}")

    # Sólo para este ejemplo tomo el primero
    selected = video_list.videos[0]
    print(f"[CLIENT] Reproduciendo: {selected.filename}")

    request = pb2.VideoRequest(filename=selected.filename)
    responses = stub.StreamVideo(request)

    for frame_msg in responses:
        jpg_bytes = np.frombuffer(frame_msg.jpeg_data, dtype=np.uint8)
        frame = cv2.imdecode(jpg_bytes, cv2.IMREAD_COLOR)

        if frame is None:
            print(f"[CLIENT] Frame {frame_msg.frame_number} inválido.")
            continue

        cv2.imshow(f"Streaming - {frame_msg.filename}", frame)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            print("[CLIENT] Se presionó 'q'. Saliendo...")
            break

    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
