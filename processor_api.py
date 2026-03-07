import os
import time
import threading
import requests
import ffmpeg
import numpy as np
import cv2 
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- CONFIGURATION ---
# Render allows writing to /tmp/ for fast, temporary ephemeral storage
WORKSPACE_DIR = "/tmp/workspace"
ASSETS_DIR = "./assets" # Make sure this folder is in your GitHub repo!

os.makedirs(WORKSPACE_DIR, exist_ok=True)
os.makedirs(ASSETS_DIR, exist_ok=True)

# --- CAMPAIGN CONFIGURATION MAP ---
CAMPAIGN_CONFIG = {
    'rajbet':   {'file': 'RajBet-LOGO.mp4',   'type': 'video', 'chroma': '0x0000FF'}, 
    'leonbet':  {'file': 'LEONBET-LOGO.mp4',  'type': 'video', 'chroma': '0x0000FF'}, 
    'tucanbit': {'file': 'TUCANBIT.mp4',      'type': 'video', 'chroma': '0x00FF00'}, 
    'bitz':     {'file': 'Bitz.io-LOGO.mp4',  'type': 'video', 'chroma': '0x00FF00'}, 
    'betstrike':{'file': 'SMART_DETECT',      'type': 'smart_image'} 
}

# --- HELPER FUNCTIONS ---
def get_brightness(video_path):
    """Calculates average brightness of the first 10 frames of video."""
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return 128
        
        brightness_values = []
        for _ in range(10): 
            ret, frame = cap.read()
            if not ret: break
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            brightness_values.append(np.mean(gray))
        
        cap.release()
        return np.mean(brightness_values) if brightness_values else 128
    except Exception as e:
        print(f"Warning: Could not calculate brightness: {e}", flush=True)
        return 128 

# --- THE BACKGROUND WORKER ---
def process_task(input_path, campaign_key, position_key, reply_webhook_url):
    timestamp = int(time.time())
    output_path = os.path.join(WORKSPACE_DIR, f"final_{timestamp}.mp4")

    try:
        print(f"⚙️ [Step 2] Processing Video | Campaign: {campaign_key} | Position: {position_key}", flush=True)

        input_stream = ffmpeg.input(input_path)
        probe = ffmpeg.probe(input_path)
        has_audio = any(stream['codec_type'] == 'audio' for stream in probe['streams'])

        processed_video = input_stream.filter(
            'scale', 1080, 1920, force_original_aspect_ratio='decrease'
        ).filter(
            'pad', 1080, 1920, '(ow-iw)/2', '(oh-ih)/2'
        )

        camp_data = CAMPAIGN_CONFIG.get(campaign_key)
        if not camp_data:
            raise ValueError(f"Unknown campaign: {campaign_key}")

        overlay_layer = None

        if camp_data['type'] == 'video':
            asset_path = os.path.join(ASSETS_DIR, camp_data['file'])
            if not os.path.exists(asset_path):
                raise FileNotFoundError(f"Missing asset: {camp_data['file']} in {ASSETS_DIR}")
            
            print(f"Applying Video Logo: {camp_data['file']} with Chroma: {camp_data['chroma']}", flush=True)
            overlay_stream = ffmpeg.input(asset_path)
            overlay_layer = (
                overlay_stream
                .filter('colorkey', camp_data['chroma'], 0.3, 0.2) 
                .filter('scale', 550, -1) 
            )

        elif camp_data['type'] == 'smart_image':
            brightness = get_brightness(input_path)
            color_suffix = "black" if brightness > 128 else "white"
            logo_filename = f"Betstrike_logo_{color_suffix}.png"
            
            asset_path = os.path.join(ASSETS_DIR, logo_filename)
            if not os.path.exists(asset_path):
                raise FileNotFoundError(f"Missing asset: {logo_filename} in {ASSETS_DIR}")

            print(f"Applying Betstrike Logo: {logo_filename} (Brightness: {brightness})", flush=True)
            overlay_layer = ffmpeg.input(asset_path).filter('scale', 550, -1)

        # Positions
        if position_key == 'top': x_val, y_val = '(main_w-overlay_w)/2', '120'
        elif position_key == 'bottom': x_val, y_val = '(main_w-overlay_w)/2', 'main_h-overlay_h-220'
        elif position_key == 'c1': x_val, y_val = '50', '120'
        elif position_key == 'c2': x_val, y_val = 'main_w-overlay_w-50', 'main_h-overlay_h-220'
        else: x_val, y_val = '(main_w-overlay_w)/2', 'main_h-overlay_h-220'

        final_video_stream = processed_video.overlay(overlay_layer, x=x_val, y=y_val)

        output_args = {
            't': '59',
            'vcodec': 'libx264',
            'crf': 23,
            'preset': 'fast',
            'strict': 'experimental'
        }
        
        if has_audio:
            output_args['acodec'] = 'aac'
            output_args['audio_bitrate'] = '128k'
            output_stream = ffmpeg.output(final_video_stream, input_stream.audio, output_path, **output_args)
        else:
            output_stream = ffmpeg.output(final_video_stream, output_path, **output_args)

        print("🎬 Rendering Final Output...", flush=True)
        ffmpeg.run(output_stream, overwrite_output=True, capture_stdout=True, capture_stderr=True)

        print(f"🚀 [Step 3] Beam back to n8n...", flush=True)
        with open(output_path, 'rb') as f:
            files = {'file': (os.path.basename(output_path), f, 'video/mp4')}
            # We return the original campaign and position so n8n knows where to post it
            data = {'campaign': campaign_key, 'position': position_key} 
            response = requests.post(reply_webhook_url, files=files, data=data)
            response.raise_for_status()

        print("✅ Delivery Successful!", flush=True)

    except ffmpeg.Error as e:
        print(f"❌ FFmpeg Error: {e.stderr.decode('utf8') if e.stderr else str(e)}", flush=True)
    except Exception as e:
        print(f"❌ Processing Error: {str(e)}", flush=True)

    finally:
        print("🧹 Cleaning up temp files...", flush=True)
        if os.path.exists(input_path): os.remove(input_path)
        if os.path.exists(output_path): os.remove(output_path)


# --- HTTP ENDPOINTS ---
@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "online", "message": "Render Video Processor is ready!"}), 200

@app.route('/process', methods=['POST'])
def process_video_api():
    # 1. Catch the file from n8n
    if 'video_file' not in request.files:
        return jsonify({"error": "No video_file uploaded"}), 400
    
    video_file = request.files['video_file']
    
    # 2. Extract the instructions n8n sent alongside the file
    campaign_key = request.form.get('campaign', 'rajbet')
    position_key = request.form.get('position', 'bottom')
    reply_webhook_url = request.form.get('webhook_reply_url')

    if not reply_webhook_url:
        return jsonify({"error": "Missing webhook_reply_url"}), 400

    print(f"⚡ [Step 1] Received video file for {campaign_key}. Saving to disk...", flush=True)
    
    # 3. Save the file temporarily so FFmpeg can edit it
    timestamp = int(time.time())
    input_path = os.path.join(WORKSPACE_DIR, f"raw_{timestamp}.mp4")
    video_file.save(input_path)
    
    # 4. Start processing in the background
    thread = threading.Thread(target=process_task, args=(input_path, campaign_key, position_key, reply_webhook_url))
    thread.daemon = True
    thread.start()

    return jsonify({"message": "File received and processing queued successfully!", "status": "processing"}), 200

if __name__ == '__main__':
    # Render assigns a dynamic port, so we pull it from the environment variables
    port = int(os.environ.get("PORT", 10000))
    print(f"Starting Video Processor Service on port {port}...", flush=True)
    app.run(host='0.0.0.0', port=port)