import logging
def set_up_log():
    logging.basicConfig(
    filename='pipeline_upload.log',
    filemode='a',                   
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
