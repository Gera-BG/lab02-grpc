import os
import grpc

import video_service_pb2 as pb2
import video_service_pb2_grpc as pb2_grpc

CHUNK_SIZE = 64 * 1024  # 64KB


def generate_upload_chunks(filepath, title):
    filename = os.path.basename(filepath)

    # 1) PRIMER CHUNK: Solo Metadata (sin data)
    yield pb2.UploadChunk(
        metadata=pb2.VideoMetadata(
            filename=filename,
            title=title
        )
    )

    # 2) CHUNKS SIGUIENTES: Solo datos binarios
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield pb2.UploadChunk(data=chunk)


def main():
    # Ruta del archivo MP4 original (fuera de la carpeta 'videos')
    local_video_path = "C:/Users/Gerardo/Videos/Video2.mp4"
    video_title = "Video1"

    if not os.path.exists(local_video_path):
        print(f"[CLIENT] No se encontró el archivo local: {local_video_path}")
        return

    # Verificar el tamaño real del archivo local
    total_bytes = os.path.getsize(local_video_path)
    print(f"[CLIENT] Tamaño del archivo a subir: {total_bytes} bytes")

    channel = grpc.insecure_channel("localhost:50051")
    stub = pb2_grpc.VideoServiceStub(channel)

    print(f"[CLIENT] Subiendo video: {local_video_path}")
    response = stub.UploadVideo(
        generate_upload_chunks(local_video_path, video_title)
    )

    print(f"[CLIENT] Upload success: {response.success}")
    print(f"[CLIENT] Message: {response.message}")
    print(f"[CLIENT] Nombre guardado en servidor: {response.stored_filename}")


if __name__ == "__main__":
    main()