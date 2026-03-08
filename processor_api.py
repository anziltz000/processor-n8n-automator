import os
import time
import threading
import queue
import requests
import ffmpeg
import numpy as np
import cv2 
from flask import Flask, request, jsonify
import yt_dlp

app = Flask(__name__)

# --- CONFIGURATION ---
WORKSPACE_DIR = "/tmp/workspace"
ASSETS_DIR = "./shared/assets/Campaigns" 

os.makedirs(WORKSPACE_DIR, exist_ok=True)
os.makedirs(ASSETS_DIR, exist_ok=True)

CAMPAIGN_CONFIG = {
    'rajbet':   {'file': 'RajBet-LOGO.mp4',   'type': 'video', 'chroma': '0x0000FF'}, 
    'leonbet':  {'file': 'LEONBET-LOGO.mp4',  'type': 'video', 'chroma': '0x0000FF'}, 
    'tucanbit': {'file': 'TUCANBIT.mp4',      'type': 'video', 'chroma': '0x00FF00'}, 
    'bitz':     {'file': 'Bitz.io-LOGO.mp4',  'type': 'video', 'chroma': '0x00FF00'}, 
    'betstrike':{'file': 'SMART_DETECT',      'type': 'smart_image'} 
}

# --- THE QUEUE SYSTEM ---
task_queue = queue.Queue()

def get_brightness(video_path):
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened(): return 128
        brightness_values = []
        for _ in range(10): 
            ret, frame = cap.read()
            if not ret: break
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            brightness_values.append(np.mean(gray))
        cap.release()
        return np.mean(brightness_values) if brightness_values else 128
    except Exception as e:
        print(f"Warning: Brightness calc failed: {e}", flush=True)
        return 128 

# ADDED target_key to arguments
def process_task(video_url, campaign_key, position_key, target_key, reply_webhook_url):
    timestamp = int(time.time())
    input_path = os.path.join(WORKSPACE_DIR, f"raw_{timestamp}.mp4")
    output_path = os.path.join(WORKSPACE_DIR, f"final_{timestamp}.mp4")
    caption = "No caption found" # Default fallback

    try:
        print(f"📥 [Step 1] Downloading: {video_url}", flush=True)
        ydl_opts = {
            'outtmpl': input_path,
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            'merge_output_format': 'mp4',
            'quiet': True,
            'no_warnings': True
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # CHANGED: extract_info grabs the description/caption during download
            info = ydl.extract_info(video_url, download=True)
            caption = info.get('description', '') or 'No caption found'

        print(f"⚙️ [Step 2] Processing Video | Campaign: {campaign_key}", flush=True)
        input_stream = ffmpeg.input(input_path)
        probe = ffmpeg.probe(input_path)
        has_audio = any(stream['codec_type'] == 'audio' for stream in probe['streams'])

        processed_video = input_stream.filter(
            'scale', 1080, 1920, force_original_aspect_ratio='decrease'
        ).filter('pad', 1080, 1920, '(ow-iw)/2', '(oh-ih)/2')

        camp_data = CAMPAIGN_CONFIG.get(campaign_key)
        overlay_layer = None

        if camp_data['type'] == 'video':
            asset_path = os.path.join(ASSETS_DIR, camp_data['file'])
            overlay_layer = ffmpeg.input(asset_path).filter('colorkey', camp_data['chroma'], 0.3, 0.2).filter('scale', 550, -2)
        elif camp_data['type'] == 'smart_image':
            brightness = get_brightness(input_path)
            logo_filename = f"Betstrike_logo_{'black' if brightness > 128 else 'white'}.png"
            overlay_layer = ffmpeg.input(os.path.join(ASSETS_DIR, logo_filename)).filter('scale', 550, -2)

        pos_map = {
            'top': ('(main_w-overlay_w)/2', '120'),
            'bottom': ('(main_w-overlay_w)/2', 'main_h-overlay_h-220'),
            'c1': ('50', '120'),
            'c2': ('main_w-overlay_w-50', 'main_h-overlay_h-220')
        }
        x_val, y_val = pos_map.get(position_key, pos_map['bottom'])

        final_video_stream = processed_video.overlay(overlay_layer, x=x_val, y=y_val)

        output_args = {'t': '59', 'vcodec': 'libx264', 'pix_fmt': 'yuv420p', 'crf': 25, 'preset': 'ultrafast', 'threads': '1', 'r': '30'}
        if has_audio:
            output_stream = ffmpeg.output(final_video_stream, input_stream.audio, output_path, **output_args, acodec='aac')
        else:
            output_stream = ffmpeg.output(final_video_stream, output_path, **output_args)

        print("🎬 Rendering Final Output...", flush=True)
        ffmpeg.run(output_stream, overwrite_output=True, capture_stdout=True, capture_stderr=True)

        print(f"🚀 [Step 3] Beaming finished video to n8n...", flush=True)
        with open(output_path, 'rb') as f:
            files = {'file': (os.path.basename(output_path), f, 'video/mp4')}
            # ADDED target and caption to the n8n payload
            data = {
                'campaign': campaign_key, 
                'position': position_key,
                'target': target_key,
                'caption': caption
            } 
            requests.post(reply_webhook_url, files=files, data=data)

        print("✅ Delivery Successful!", flush=True)

    except ffmpeg.Error as e:
        print(f"❌ FFmpeg Error: {e.stderr.decode('utf8') if e.stderr else str(e)}", flush=True)
    except Exception as e:
        print(f"❌ Processing Error: {str(e)}", flush=True)

    finally:
        print("🧹 Cleaning up temp files...", flush=True)
        if os.path.exists(input_path): os.remove(input_path)
        if os.path.exists(output_path): os.remove(output_path)


# --- BACKGROUND WORKER ENGINE ---
def worker():
    """This engine runs in the background and processes the queue one by one."""
    while True:
        task = task_queue.get()
        if task is None:
            break
        print(f"🚦 Pulling task from queue: {task['campaign']}...", flush=True)
        # ADDED task['target']
        process_task(task['url'], task['campaign'], task['position'], task['target'], task['webhook_reply_url'])
        task_queue.task_done()

# Boot up the single background worker thread
worker_thread = threading.Thread(target=worker, daemon=True)
worker_thread.start()


# --- HTTP ENDPOINTS ---
@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "online", "queue_size": task_queue.qsize()}), 200

@app.route('/process', methods=['POST'])
def process_video_api():
    data = request.json
    video_url = data.get('url')
    campaign_key = data.get('campaign', 'rajbet')
    position_key = data.get('position', 'bottom')
    target_key = data.get('target', 'upload_both') # EXTRACT target
    reply_webhook_url = data.get('webhook_reply_url')

    if not all([video_url, reply_webhook_url]):
        return jsonify({"error": "Missing URL or Reply Webhook"}), 400

    # Put the job in the queue
    task_queue.put({
        'url': video_url,
        'campaign': campaign_key,
        'position': position_key,
        'target': target_key, # STORE target in queue
        'webhook_reply_url': reply_webhook_url
    })

    print(f"📋 Task added to queue. Current line: {task_queue.qsize()}", flush=True)
    return jsonify({"message": "Added to queue!", "queue_position": task_queue.qsize()}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
