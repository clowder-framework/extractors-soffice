# text extractor wrapper
# wraps process_pdf.py

import logging
import os
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
        # Use deterministic output name for the converted file.
        converted_basename = os.path.splitext(os.path.basename(input_file))[0] + ".pdf"
        output_pdf_filename = input_filename + ".pdf"
        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")

        # # Call soffice and force the output directory so we know where PDF lands.
        # convert_result = subprocess.run(
        #     [
        #         'soffice',
        #         '--headless',
        #         '--convert-to',
        #         'pdf',
        #         '--outdir',
        #         input_dir,
        #         input_file
        #     ],
        #     capture_output=True,
        #     text=True
        # )
        
        # Use isolated profile/outdir per job to avoid soffice profile lock conflicts.
        with tempfile.TemporaryDirectory(prefix="soffice-profile-") as profile_dir, \
             tempfile.TemporaryDirectory(prefix="soffice-out-") as convert_outdir:
            output_file = os.path.join(convert_outdir, converted_basename)
            convert_result = subprocess.run(
                [
                    'soffice',
                    '--headless',
                    '--nologo',
                    '--nodefault',
                    '--nolockcheck',
                    '--convert-to',
                    'pdf',
                    '--outdir',
                    convert_outdir,
                    '-env:UserInstallation=%s' % Path(profile_dir).as_uri(),
                    input_file
                ],
                capture_output=True,
                text=True
            )

            if convert_result.returncode != 0 and not os.path.exists(output_file):
                log.error("soffice conversion failed for %s", input_file)
                if convert_result.stdout:
                    log.error("soffice stdout: %s", convert_result.stdout.strip())
                if convert_result.stderr:
                    log.error("soffice stderr: %s", convert_result.stderr.strip())
                raise RuntimeError("soffice failed to convert input file to PDF")

            if convert_result.returncode != 0 and os.path.exists(output_file):
                log.warning("soffice returned non-zero but PDF exists for %s", input_file)
                if convert_result.stderr:
                    log.warning("soffice stderr: %s", convert_result.stderr.strip())

            if not os.path.exists(output_file):
                raise FileNotFoundError("Converted PDF not found: %s" % output_file)

            # Rename output file to match the desired PDF filename for upload.
            log.info("Output file: %s", output_file)
            log.info("Output PDF filename: %s", output_pdf_filename)
            os.replace(output_file, output_pdf_filename)

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
        log.info("SOffice PDF file ID: %s", pdf_fileid)
        # upload metadata to dataset
        extracted_files = {
            "input_word_file": {
                "file_id": input_file_id,
                "filename": input_filename,
                "description": "Input word file"
            },
            "output_pdf_file": {
                "file_id": pdf_fileid,
                "filename": output_pdf_filename,
                "description": "PDF output file"
            }
        }
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
