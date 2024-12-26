import data_collection
import schedule
import archive

import traceback
import logging
import platform
from datetime import datetime
import os
import smtplib
from email.message import EmailMessage

logger = logging.getLogger('run')

log_location = None

def send_failure_email(step, exception_traceback):
    logger.info('Sending failure email')

    # read email credentials
    with open(('\\' if platform.system() == 'Windows' else '/').join(['data', 'email_credentials.txt']), 'r') as f:
        host = f.readline().replace('\n', '')
        email = f.readline().replace('\n', '')
        password = f.readline().replace('\n', '')
        error_email = f.readline().replace('\n', '')

    msg = EmailMessage()

    msg['From'] = email
    msg['To'] = error_email
    msg['Subject'] = f'Movie theater breakdown failed at step {step} at {datetime.now().strftime("%m/%d/%Y %H:%M:%S")}' 


    with open(log_location, 'r') as f:
        log = f.read()

    msg.set_content(f'Exception traceback:\n{str(exception_traceback)}\n\n\nPlease check logs for more information.')

    # initialize smtp connection
    server = smtplib.SMTP(host, 587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(email, password)

    server.send_message(msg)

    server.quit()

    logger.info('Failure notification sent')
    return

def run():
    try:
        start_time = datetime.now()

        log_location = ('\\' if platform.system() == 'Windows' else '/').join(['logs', f'movie_schedule_{datetime.now().strftime("%d%m%Y")}.log'])
    
        if(not os.path.isfile(log_location)):
            open(log_location, 'w+')
        else:
            with open(log_location, 'a') as f:
                f.write('\n\n\n')

        logging.basicConfig(filename=log_location, level=logging.INFO)
        logger.info(f'Starting {start_time.strftime("%m/%d/%Y %H:%M:%S")}')


        logger.info('Starting data collection')
        step = 'data_collection'
        data_collection.run()
        logger.info('Data collection done')

        logger.info('Starting schedule')
        step = 'schedule'
        schedule.run()
        logger.info('Schedule done')

        logger.info('Starting archive')
        step = 'archive'
        archive.run()
        logger.info('Archive done')
    except Exception:
        logger.error(traceback.format_exc())
        send_failure_email(step, traceback.format_exc())
    finally:
        end_time = datetime.now()
        logger.info(f'Finished {end_time.strftime("%m/%d/%Y %H:%M:%S")}, total runtime: {(end_time-start_time).total_seconds()} seconds')
    


if __name__ == "__main__":
    run()