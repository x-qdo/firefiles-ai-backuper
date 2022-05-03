import errno
import os

import requests
import argparse
import re
import csv
from datetime import datetime


def save_meeting_sentences_to_csv(filepath, sentences):
    with open(filepath, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['index', 'raw_text', 'start_time', 'end_time', 'speaker_id', 'speaker_name'])
        for sentence in sentences:
            writer.writerow([sentence['index'], sentence['raw_text'], sentence['start_time'], sentence['end_time'],
                             sentence['speaker_id'], sentence['speaker_name']])


def get_meetings_info(skip, limit, api_key):
    url = 'https://api.fireflies.ai/graphql'
    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    response = requests.post(url, headers=headers, json={'query': '''
        query {
           transcripts(limit:%d, skip:%d) {
               id
               title
               participants
               date
               transcript_url
               duration
               sentences {
                index
                raw_text
                start_time
                end_time
                speaker_id
                speaker_name
               }
           }
        }
    ''' % (limit, skip)})
    return response.json()


def get_audio_url(meeting_id):
    return 'https://rtmp-server-ff.s3.amazonaws.com/{0}/audio.mp3' \
        .format(meeting_id)


def get_folder_path(backup_location, meeting_date):
    folder_path = '{}/{}'.format(backup_location, datetime.utcfromtimestamp(meeting_date / 1000).strftime('%Y-%m'))
    if not os.path.exists(folder_path):
        try:
            os.makedirs(folder_path)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    return folder_path


def get_filename(meeting_date, meeting_title, extension='mp3'):
    return '{}-{}.{}'.format(datetime.utcfromtimestamp(meeting_date / 1000).strftime('%Y-%m-%d_%H_%M_%S'),
                             re.sub(r"[^A-Za-z]", "", meeting_title, 0, re.MULTILINE),
                             extension)


def delete_meeting(token, meeting_id):
    api_key = token
    url = 'https://api.fireflies.ai/graphql'
    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    response = requests.post(url, headers=headers, json={'query': '''
            mutation($transcriptId: String!) {
              deleteTranscript(id: $transcriptId) {
                title
                date
                duration
                organizer_email
              }
            }
        ''', 'variables': {'transcriptId': meeting_id}})
    print(response.json())


def backup_meetings(token, backup_location, cleanup_transcript=False, backup_transcript=True, skip=50):
    api_key = token
    current_skip = skip
    limit = 10
    while True:
        response = get_meetings_info(current_skip, limit, api_key)
        if 'errors' in response:
            print(response['errors'])
            break
        for meeting in response['data']['transcripts']:
            print('backing up meeting ID:{} {}'.format(meeting['id'], get_filename(meeting['date'], meeting['title'])))

            if backup_transcript and meeting['sentences'] is not None:
                save_meeting_sentences_to_csv('{}/{}'.format(get_folder_path(backup_location, meeting['date']),
                                                             get_filename(meeting['date'], meeting['title'],
                                                                          'transcript.csv')),
                                              meeting['sentences'])

            r = requests.get(get_audio_url(meeting['id']))
            with open('{}/{}'.format(get_folder_path(backup_location, meeting['date']),
                                     get_filename(meeting['date'], meeting['title'])), 'wb') as f:
                f.write(r.content)

            if cleanup_transcript:
                delete_meeting(token, meeting['id'])

        if len(response['data']['transcripts']) < limit:
            break

        current_skip += limit


parser = argparse.ArgumentParser(description='FireFiles backup tool')
parser.add_argument('--token', required=True, help='Personal auth topic. Can be found '
                                                   'https://app.fireflies.ai/integrations/custom/fireflies')
parser.add_argument('--backup-location', default='./backup')
parser.add_argument('--backup-transcript', action='store_false')
parser.add_argument('--cleanup-transcript', action='store_true')
parser.add_argument('--skip', default=50, type=int, help='Number of meetings to skip from start')

args = parser.parse_args()
backup_meetings(**vars(args))
