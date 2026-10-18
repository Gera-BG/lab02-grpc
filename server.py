import os
import time
from concurrent import futures

import cv2
import grpc

import video_service_pb2 as pb2
import video_service_pb2_grpc as pb2_grpc

VIDEOS_DIR = "videos"


class VideoServiceServicer(pb2_grpc.VideoServiceServicer):
    def __init__(self):
        os.makedirs(VIDEOS_DIR, exist_ok=True)

    def UploadVideo(self, request_iterator, context):
        metadata = None
        file_path = None
        file = None

        try:
            for chunk in request_iterator:
                # 1. Si el mensaje trae metadata
                if chunk.HasField("metadata"):
                    metadata = chunk.metadata
                    safe_name = os.path.basename(metadata.filename)
                    file_path = os.path.join(VIDEOS_DIR, safe_name)
                    file = open(file_path, "wb")
                    print(f"[SERVER] Recibiendo video: {metadata.filename} ({metadata.title})")

                # 2. Si el mensaje trae datos binarios
                elif chunk.HasField("data"):
                    if file is None:
                        context.set_details("No metadata received before data.")
                        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                        return pb2.UploadStatus(success=False, message="No metadata before data")
                    file.write(chunk.data)

            if file:
                file.close()

            if metadata is None or file_path is None:
                return pb2.UploadStatus(success=False, message="No metadata received")

            file_size = os.path.getsize(file_path)
            print(f"[SERVER] Video guardado en: {file_path} ({file_size} bytes)")

            return pb2.UploadStatus(
                success=True,
                message="Video uploaded successfully.",
                stored_filename=os.path.basename(file_path)
            )

        except Exception as e:
            if file and not file.closed:
                file.close()
            print(f"[SERVER] Error en UploadVideo: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return pb2.UploadStatus(success=False, message=str(e))
            
    def ListVideos(self, request, context):
        print("[SERVER] Listando videos...")
        videos = []

        if os.path.exists(VIDEOS_DIR):
            for fname in os.listdir(VIDEOS_DIR):
                if fname.lower().endswith((".mp4", ".avi", ".mkv", ".mov")):
                    info = pb2.VideoInfo(
                        filename=fname,
                        title=fname
                    )
                    videos.append(info)

        return pb2.VideoList(videos=videos)

    def StreamVideo(self, request, context):
        filename = request.filename
        video_path = os.path.join(VIDEOS_DIR, filename)

        print(f"[SERVER] StreamVideo solicitado: {video_path}")

        # 1. Verificar existencia del archivo
        if not os.path.exists(video_path):
            context.set_details(f"Video not found: {filename}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return

        # 2. Verificar que el archivo no esté vacío (0 bytes)
        if os.path.getsize(video_path) == 0:
            print(f"[SERVER] Error: El archivo {filename} tiene 0 bytes.")
            context.set_details(f"Video file is empty (0 bytes): {filename}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return

        # 3. Abrir video con OpenCV
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"[SERVER] OpenCV no pudo abrir: {video_path}")
            context.set_details(f"Cannot open video: {filename}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return

        frame_number = 0
        fps = cap.get(cv2.CAP_PROP_FPS)
        delay = 1.0 / fps if fps and fps > 0 else 0.03

        while True:
            band, frame = cap.read()
            if not band:
                print(f"[SERVER] Fin del video: {filename}")
                break

            success, buffer = cv2.imencode(".jpg", frame)
            if not success:
                continue

            yield pb2.VideoFrame(
                jpeg_data=buffer.tobytes(),
                frame_number=frame_number,
                filename=filename
            )
            frame_number += 1

            time.sleep(delay)

        cap.release()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_VideoServiceServicer_to_server(VideoServiceServicer(), server)

    server.add_insecure_port("[::]:50051")
    print("[SERVER] VideoService corriendo en puerto 50051...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()