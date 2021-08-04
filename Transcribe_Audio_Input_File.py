import sys
import os
import json
import pandas as pd
import hashlib
import argparse
from pandas import json_normalize
from datetime import datetime
from pydub.utils import mediainfo
import fleep

from google.cloud import speech_v1p1beta1 as speech
from google.cloud import storage

#Set the path for GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/mnt/ssd/home/ec2-user/w210-318705-903e643a946c.json"

#Get the input arguments
parser = argparse.ArgumentParser()
parser.add_argument("--audio_file_name", help="name of the input audio file")
parser.add_argument("--dest_bucket_name", help="name of the GCS bucket used to store audio files", default="w210-tl-dl-bucket")
parser.add_argument("--hash_registry_table_name", help="path of the audio file hash registry", default="transcript_md5_hash_registry.csv")
parser.add_argument("--transcript_dir_name", help="path where the transcripts are saved", default="Transcripts/")
args = parser.parse_args()

#Create timestamp string
dt_string = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
timestamped_audio_file_name = args.audio_file_name + " - " + dt_string

#Create destination bucket string
gcs_destination_location = "gs://" + args.dest_bucket_name + "/" + timestamped_audio_file_name

#Get sample rate of audio file (this is nercessary to execute the transcription process)
input_sample_rate = int(mediainfo(args.audio_file_name)['sample_rate'])
channel_count = int(mediainfo(args.audio_file_name)['channels'])

#Get the file extension type
#with open(audio_file_name, "rb") as file:
#    info = fleep.get(file.read(128))
#input_file_extension = info.extension[0]

input_file_extension = mediainfo(args.audio_file_name)['codec_name']

if input_file_extension == "flac":
    encoding_string = speech.RecognitionConfig.AudioEncoding.FLAC
elif input_file_extension == "mp3":
    encoding_string = speech.RecognitionConfig.AudioEncoding.MP3
else:
    encoding_string = speech.RecognitionConfig.AudioEncoding.ENCODING_UNSPECIFIED

#Function for uploading file to GCS bucket
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

#Functions for Google Speech to Text API transcription
def transcribe_gcs(gcs_uri):
    """Asynchronously transcribes the audio file specified by the gcs_uri."""
    client = speech.SpeechClient()

    audio = speech.RecognitionAudio(uri=gcs_uri)
    config = speech.RecognitionConfig(
        encoding=encoding_string,
        sample_rate_hertz=input_sample_rate,
        audio_channel_count=channel_count,
        use_enhanced=True,
        model="video",
        language_code="en-US",
        enable_word_time_offsets=True,
        enable_automatic_punctuation = True
    )

    operation = client.long_running_recognize(config=config, audio=audio)

#    print("Waiting for operation to complete...")
    result = operation.result(timeout=3600)
    result_json = type(result).to_json(result)
    return result_json

    # Each result is for a consecutive portion of the audio. Iterate through
    # them to get the transcripts for the entire audio file.
#    for result in response.results:
        # The first alternative is the most likely one for this portion.
#        print(u"Transcript: {}".format(result.alternatives[0].transcript))
#        print("Confidence: {}".format(result.alternatives[0].confidence))

def reconstitute_transcript(input_json):
    transcription_result_json = pd.read_json(input_json)

    full_transcript = ''

    for i in range(len(transcription_result_json.results)):
        line = json_normalize(json_normalize(transcription_result_json.results[i])['alternatives'][0])['transcript'][0]
        full_transcript += line.replace('. ', '. \n')

    return(full_transcript)

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

#Check MD5 hash against registry table
audio_file_hash = md5(args.audio_file_name)
hash_registry_pd = pd.read_csv(args.hash_registry_table_name)
transcript_filename_check = timestamped_audio_file_name + ".txt"
hash_query = hash_registry_pd[hash_registry_pd["md5_hash"] == audio_file_hash]

#If no match found:
if len(hash_query) == 0:
    print("No matching hash found; uploading to GCS and executing trancription")
    
    #Send uploaded audio file to GCS Bucket
    upload_blob(bucket_name = args.dest_bucket_name,
        source_file_name = args.audio_file_name,destination_blob_name = timestamped_audio_file_name)

    #Execute transcription on uploaded audio file from GCS Bucket
    transcription_result = transcribe_gcs(gcs_destination_location)

    #Save transcript to TLDL Pipeline/Transcripts
    text_file = open(args.transcript_dir_name + timestamped_audio_file_name + ".txt", "w")
    text_file.write(transcription_result)
    text_file.close()

    #Write transcript name, MD5 hash to registry table
    with open(args.hash_registry_table_name,'a') as fd:
        fd.write(timestamped_audio_file_name + ".txt," + audio_file_hash + "\n")

#If match found:
else:
    #Open file associated with existing hash
    matching_hash_filename = hash_registry_pd[hash_registry_pd["md5_hash"] == audio_file_hash].iloc[0][0]
    print("Hash match found in file " + matching_hash_filename)
    print("Retrieving existing transcript")
    
    with open(args.transcript_dir_name + matching_hash_filename, 'r') as f2:
        transcription_result = f2.read()

#Create transcript from variable
transcript = reconstitute_transcript(transcription_result)

#write transcript to a local .txt file
f = open("model_transcript.txt", "w")
f.write(transcript)
f.close()