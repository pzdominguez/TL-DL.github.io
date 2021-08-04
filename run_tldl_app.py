import os
import time
import argparse
import streamlit as st

st.header("Welcome to TL;DL")
st.write("Once you upload your podcast audio file, we'll generate a summary based on the audio.")

# now = time.strftime('%a, %d %b %Y %H:%M:%S %Z(%z)')
parser = argparse.ArgumentParser()
parser.add_argument("--audio_file_name", help="name of the input audio file")
args = parser.parse_args()

def execute_tldl(upload_file):
    os.system('python Transcribe_Audio_Input_File.py --audio_file_name ' + upload_file + ' --dest_bucket_name w210-tl-dl-bucket --hash_registry_table_name transcript_md5_hash_registry.csv --transcript_dir_name Transcripts/')
    os.system('python model_output.py --article model_transcript.txt --model_dir model_multi_news/ --model_name multi_news')
    model_output = open("model_output.txt")
    summary = model_output.read().replace("\n", " ")
    model_output.close()
    return summary

fileObject = st.file_uploader(label="Please upload your mp3 file")

if fileObject:

    # Save the uploaded file
    # file_name = "_".join([fileObject.name, now])
    file_details = {"FileName": fileObject.name, "FileType": fileObject.type}
    with open(os.path.join("/mnt/ssd/home/ec2-user/TLDL Pipeline", fileObject.name),"wb") as f: 
        f.write(fileObject.getbuffer())         
    st.success("Saved File")
    args.audio_file_name = fileObject.name
    
    # Set up result and progress bar
    result = {}
    sleep_duration = 1
    percent_complete = 0
    st.text("Currently in queue")
    progress_bar = st.progress(percent_complete)
    
    while result.get("status") != "processing":
        percent_complete += sleep_duration
        time.sleep(sleep_duration)
        progress_bar.progress(percent_complete / 10)
        ## Run code here
        summary = execute_tldl(args.audio_file_name)
        result["status"] = "processing"
        result["text"] = summary

    sleep_duration = 0.01

    for percent in range(percent_complete, 101):
        time.sleep(sleep_duration)
        progress_bar.progress(percent)

    st.balloons()
    st.header("Here's the summary of your podcast:")
    st.subheader(result['text'])