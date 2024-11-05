# text extractor wrapper
# wraps process_pdf.py

import time
import logging
import os
from datetime import datetime
import subprocess

from pyclowder.extractors import Extractor
import pyclowder.files

# create log object with current module name
log = logging.getLogger(__name__)


class SofficeExtractor(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # # setup logging for the exctractor
        # logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        # logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, connector, host, secret_key, resource, parameters):
        # Process the file and upload the results
        # uncomment to see the resource
        # log.info(resource)
        # {'type': 'file', 'id': '6435b226e4b02b1506038ec5', 'intermediate_id': '6435b226e4b02b1506038ec5', 'name': 'N18-3011.pdf', 'file_ext': '.pdf', 'parent': {'type': 'dataset', 'id': '64344255e4b0a99d8062e6e0'}, 'local_paths': ['/tmp/tmp2hw6l5ra.pdf']}

        input_file = resource["local_paths"][0]
        input_file_id = resource['id']
        dataset_id = resource['parent'].get('id')
        input_filename = os.path.splitext(os.path.basename(resource["name"]))[0]
        input_file_ext = resource['file_ext']
        output_file = os.path.join(os.path.splitext(os.path.basename(input_file))[0] + ".pdf")
        output_pdf_filename = os.path.join(input_filename + ".pdf")
        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")
        
        # call soffice to convert to pdf
        subprocess.call( ['soffice',
                 '--headless',
                 '--convert-to',
                 'pdf',
                 input_file ] ) 
        # Rename the output file to match the desired PDF filename
        subprocess.call(['mv', output_file, output_pdf_filename])

        log.info("Output Pdf file generated : %s", output_pdf_filename)
        connector.message_process(resource, "Word to pdf conversion completed.")

        # clean existing duplicate
        files_in_dataset = pyclowder.datasets.get_file_list(connector, host, secret_key, dataset_id)
        for file in files_in_dataset:
            if file["filename"] == output_pdf_filename:
                url = '%sapi/files/%s?key=%s' % (host, file["id"], secret_key)
                connector.delete(url, verify=connector.ssl_verify if connector else True)
        connector.message_process(resource, "Check for duplicate files...")

        # upload to clowder
        connector.message_process(resource, "Uploading output files to Clowder...")
        pdf_fileid = pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, output_pdf_filename)
        # upload metadata to dataset
        extracted_files = [
            {"file_id": input_file_id, "filename": input_filename, "description": "Input word file"},
            {"file_id": pdf_fileid, "filename": output_pdf_filename, "description": "PDF output file"},
        ]
        content = {"extractor": "soffice-extractor", "extracted_files": extracted_files}
        context = "http://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"
        #created_at = datetime.now().strftime("%a %d %B %H:%M:%S UTC %Y")
        user_id = "http://clowder.ncsa.illinois.edu/api/users"  # TODO: can update user id in config
        agent = {"@type": "user", "user_id": user_id}
        metadata = {"@context": [context], "agent": agent, "content": content}
        pyclowder.datasets.upload_metadata(connector, host, secret_key, dataset_id, metadata)


if __name__ == "__main__":
    # uncomment for testing
    # input_file = "data/Building.docx"
    # subprocess.call( ['soffice',
    #              '--headless',
    #              '--convert-to',
    #              'pdf',
    #              input_file ] )

    extractor = SofficeExtractor()
    extractor.start()
