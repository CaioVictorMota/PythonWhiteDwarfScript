#!/usr/bin/python3.7

"""WHITEDWARF
PGDASD Files parser and Branches extractor.
Script used to parse through PGDASD files and extract off county
companies with branches in a county determined by the cod_tom
value.
Author: Caio Victor

Current Version: 3.0
CHANGES:
    - Id's of files to be processed now are gathered in a list, instead of
        holding the connection
    - Connection setup as a global, so it will not be passed around
    - If folders used to download and process files doesn't exist, they
        will be created automatically. Their path can be chosen by altering
        the System Parameters "DOWNLOAD_LOCAL" and "EXTRACTION_LOCAL"
    - Script Arguments have been implemented! The list of arguments that can
        be passed are listed on the Arguments section.
            - "-deb" (Delete Empty Branches): This Argument deletes all
            processed branches files files under a given size in bytes. This
            size can be adjusted by the System Parameter "FILE_CLEANSE_SIZE".
            This is used to cleanse processed files without any useful data.
            - "-dtmp" (Delete TMP Files): This Argument deletes all temporary
            files that are downloaded from the database, as well as the folder
            in which they are downloaded, after their processing.
            - "-vm" (Verbose Messaging): This Argument prints more messages and
            counts during the script run.
            - "-pr" (Print Report): This Argument Prints a brief report at the
            end of processing.
            - "-LR" (Long Run Mode): This is a run mode in which the script
            will attempt to process as many files as possible. If it finds
            an error, it will store it and attempt to process a file again.

PARAMETERS AND CONSTANTS
- SYSTEM MESSAGES
    - These parameters are used to display the various messages during the
    script run. There is no reason to change them.

- CONNECTION PROPERTIES
    - These parameters hold information about the database to be connected.
    They SHOULD be checked and changed accordingly. CONNECTION is a special
    case, as it only serves to hold the connection to the database and will
    be used during the script execution.

- ARGUMENTS
    - These parameters are used to compare and setup argument options during
    the script execution. They SHOULD NOT be changed.

- SYSTEM PARAMETERS
    - These parameters control some aspects of the script execution. Parameters
    under the "Totalizers and data holders" tag SHOULD NOT be changed, as they
    receive data during the execution.
    - Parameters under the "mutable parameters" tag MAY be changed to better
    fit the behavior of the script.
        - "DOWNLOAD_LOCAL": This determines the path in which files from the
        database will be downloaded and extracted. It SHOULD end with a '\'
        - "EXTRACTION_LOCAL": This determines the path in which processed files
        will be stored. It SHOULD end with a '\'
        - "PROCESSED_FILES_PREFIX":
        - "FILES_LIMIT": This determines the amount of files that will be
        extracted and parsed from the database. If left at '0' all files will
        be extracted/parsed.
        - "FILES_OFFSET": This determines the offset of files that will be
        extracted and parsed from the database. If left at '0' the script will
        start at the beginning.
        - "SIZE_ASC_DESC": This swaps the ordering of files that will be
        extracted and parsed from the database based on the size of the files.
        'asc' orders file from the smallest to the biggest, and 'desc' does the
        opposite.
        - "TARGET_COD_TOM": This determines which CodTom the script will look
        for when processing files. Each county has its own and this parameter
        SHOULD be changed to match which branches will be processed.
        This number SHOULD start and end with '|' or it will result in
        errors. Ex: '|9999|'
        - FILE_CLEANSE_SIZE": This is the size in bytes of processed files that
        will be deleted if the argument '-deb' or '-LR' are activated. This
        size reflects a processed file with no useful information. The base
        value is '50' and should only be changed if the arguments are not
        working properly (Ex: files are not being deleted due to be bigger)
        - "MAIN_QUERY": This parameter holds the Main Query in which the list
        of files id's will be extracted and parsed. It should only be changed
        if the database has a significative difference that may impact the
        results, otherwise should be left unchanged, as it also is built
        around other parameters.
        - "EXTRACT_QUERY": This parameter holds a simple query used to grab a
        file from the target database. It should only be changed if the
        database has a significative difference that may impact the results
        , otherwise should be left unchanged.
        - "FILE_TYPE_QUERY": This parameter holds a query which is used to
        retrieve the id of PGDASD type files in the database. It should only be
        changed if the database has a significative difference that may impact
        the results, otherwise should be left unchanged.

"""

import sys
import os
import traceback
import psycopg2
from zipfile import ZipFile

# SYSTEM MESSAGES
MSG_SCRIPT_START = "\nSTARTING PGDASD BRANCHES PARSER\n"
# Support Indentation
ID2 = "  "
ID4 = "    "
ID6 = "      "
ID8 = "        "
# load_arguments
MSG_LOADING_ARGS = ID2 + "Loading Arguments..."
MSG_LOADING_STANDARD_ARGS = (ID2 + "No Arguments Found. "
                             "Loading Standard Parameters")
MSG_ARGS_LOAD_SUCCESS = ID2 + "Arguments loaded successfully"
MSG_VM_VERBOSE_SETUP = ID4 + "Verbose Messaging: "
MSG_VM_REPORT_SETUP = ID4 + "Report Print: "
MSG_VM_DELETE_EMPTY_SETUP = ID4 + "Delete Empty Branches Files: "
MSG_VM_DELETE_TMP_SETUP = ID4 + "Delete Tmp Files After Processing: "
MSG_VM_LONG_RUN_SETUP = ID4 + "Long Run Mode: "
# setup_environment
MSG_SETUP_ENVIRONMENT = ID2 + "Setting up Environment..."
MSG_SETUP_ENVIRONMENT_SUCCESS = ID2 + "Environment setup complete."
MSG_LONG_RUN_ACTIVATED = ("\n" + ID4 + "LONG RUN MODE ACTIVATED!\n" + ID4
                          + "ERRORS WILL BE SUPPRESSED, AND THIS PROGRAM\n"
                          + ID4 + "WILL FORCE THE PROCESSING OF FILES\n"
                          + ID4 + "INDEFINITELY POSSIBLY CAUSING OVERLOAD!\n")
MSG_VM_TMP_FILES_FOLDER = ID4 + "Files will be download at "
MSG_VM_BRANCHES_FILES_FOLDER = ID4 + "Files will be processed at "
# database_connect
MSG_DATABASE_CONNECT = ID2 + "Connecting to Database..."
MSG_DATABASE_CONNECT_SUCCESS = ID2 + "Database successfully connected"
# execute_main_query
MSG_RUN_MAIN_QUERY = ID2 + "Running Main Query..."
MSG_RUN_MAIN_QUERY_SUCCESS = ID2 + "Main Query successful"
MSG_VM_TOTAL_FILES_FOUND = ID4 + "Total Files to be processed:"
# download_extract_file
MSG_DOWNLOAD_FILE_NEXT = ID4 + "Downloading Next File..."
MSG_DOWNLOAD_FILE_SUCCESS = ID4 + "File Downloaded: "
# process_zip_file
MSG_ZIP_FILE_FOUND = ID6 + "Zip file found: Unzipping"
# process_text_file
MSG_PROCESS_FILE = ID6 + "Processing file: "
MSG_PROCESS_FILE_SUCCESS = ID6 + "File Processed Successfully"
MSG_VM_TOTAL_COMPANIES_FOUND = ID8 + "Total Companies Found: "
MSG_VM_NO_COMPANIES_FOUND = (ID8 + "No Companies found "
                             "according to search parameters")
MSG_VM_TOTAL_COMPANIES_RECORDED = ID8 + "Total Companies Recorded: "
# process_end
MSG_PROCESS_FINISHED = ID2 + "Processing Finished Successfully!"
MSG_ERROR_PROCESS_FINISHED = ID2 + "Processing finished with an error!"
MSG_TOTAL_FILES_DOWNLOADED = ID2 + "Total Files Downloaded: "
MSG_TOTAL_FILES_PARSED = ID2 + "Total Files Parsed: "
MSG_TOTAL_FILES_PROCESSED = ID2 + "Total Files Processed: "
MSG_LAST_FILE_SUCCESSFULLY_PARSED = ID2 + "Last File Successfully Parsed: "
# long_run
MSG_ERROR_LONG_RUN = ID2 + "Error while processing file! "
MSG_LONG_RUN_RETRY = ID2 + "Long Run will Restart this file processing\n"
MSG_LONG_RUN_END = ID2 + "Long Run has ended. Results:"
MSG_LONG_RUN_TOTAL_ERRORS = ID2 + "Total Errors: "
MSG_LONGRUN_ERROR_LIST = ID2 + "List of Errors: "
MSG_LONG_RUN_ALERT = (ID2 + "This script is running on Long Run Mode\n"
                      + ID2 + "Consider ending it if errors persist")
# cleanup_environment
MSG_CLEANUP_ENVIRONMENT = ID2 + "Environment cleaned up"

# CONNECTION PROPERTIES
HOST = "HOST"
PORT = "PORT"
DATABASE = "DATABASE"
USER = "USER"
PASSWORD = "PASSWORD"
CONNECTION = False

# ARGUMENTS
ARG_VERBOSE_MESSAGING = "-vm"
ARG_DELETE_EMPTY_BRANCHES_FILES = "-deb"
ARG_DELETE_TMP_FILES_AFTER_PROCESS = "-dtmp"
ARG_PRINT_REPORT = "-pr"
ARG_LONG_RUN = "-LR"

VERBOSE_MESSAGING = False
DELETE_EMPTY_BRANCHES_FILES = False
DELETE_TMP_FILES_AFTER_PROCESS = False
PRINT_REPORT = False
LONG_RUN = False

# SYSTEM PARAMETERS
# Totalizers and data holders
FILE_TYPE_ID = "ID"
LONG_RUN_ALERT_COUNT = 0
TOTAL_FILES_PARSED = 0
TOTAL_FILES_DOWNLOADED = 0
TOTAL_FILES_PARSED = 0
TOTAL_FILES_PROCESSED = 0
TOTAL_ERRORS_FOUND_LONG_RUN = 0
ERRORS_LIST_LONG_RUN = []
LAST_FILE_SUCCESSFULLY_PARSED = "FILE"
# mutable parameters
DOWNLOAD_LOCAL = "tmpfiles/"
EXTRACTION_LOCAL = "filiais/"
PROCESSED_FILES_PREFIX = "filiais_"
FILES_LIMIT = "0"
FILES_OFFSET = "0"
ASC_DESC = "desc"
TARGET_COD_TOM = "|3685|"
FILE_CLEANSE_SIZE = 50
MAIN_QUERY = ("MAIN_QUERY" + FILE_TYPE_ID)
EXTRACT_QUERY = ("EXTRACT_QUERY")
FILE_TYPE_QUERY = ("FILE_TYPE_QUERY")


# -----------------------------------------------------------------------------
def load_arguments():
    print(MSG_LOADING_ARGS)
    try:
        ARGUMENTS_FOUND = False
        global VERBOSE_MESSAGING
        global DELETE_EMPTY_BRANCHES_FILES
        global DELETE_TMP_FILES_AFTER_PROCESS
        global PRINT_REPORT
        global LONG_RUN
        for arg in sys.argv:
            if arg == ARG_VERBOSE_MESSAGING:
                VERBOSE_MESSAGING = True
                ARGUMENTS_FOUND = True
            elif arg == ARG_DELETE_EMPTY_BRANCHES_FILES:
                DELETE_EMPTY_BRANCHES_FILES = True
                ARGUMENTS_FOUND = True
            elif arg == ARG_DELETE_TMP_FILES_AFTER_PROCESS:
                DELETE_TMP_FILES_AFTER_PROCESS = True
                ARGUMENTS_FOUND = True
            elif arg == ARG_PRINT_REPORT:
                PRINT_REPORT = True
                ARGUMENTS_FOUND = True
            elif arg == ARG_LONG_RUN:
                LONG_RUN = True
                PRINT_REPORT = True
                DELETE_EMPTY_BRANCHES_FILES = True
                DELETE_TMP_FILES_AFTER_PROCESS = True
                VERBOSE_MESSAGING = False
                ARGUMENTS_FOUND = True
                break
        if not ARGUMENTS_FOUND:
            print(MSG_LOADING_STANDARD_ARGS)
            LONG_RUN = False
            PRINT_REPORT = False
            DELETE_EMPTY_BRANCHES_FILES = False
            DELETE_TMP_FILES_AFTER_PROCESS = False
            VERBOSE_MESSAGING = False
        print(MSG_ARGS_LOAD_SUCCESS)
        if VERBOSE_MESSAGING:
            print(MSG_VM_VERBOSE_SETUP, VERBOSE_MESSAGING)
            print(MSG_VM_REPORT_SETUP, PRINT_REPORT)
            print(MSG_VM_DELETE_EMPTY_SETUP, DELETE_EMPTY_BRANCHES_FILES)
            print(MSG_VM_DELETE_TMP_SETUP, DELETE_TMP_FILES_AFTER_PROCESS)
            print(MSG_VM_LONG_RUN_SETUP, LONG_RUN)
        if LONG_RUN:
            print(MSG_LONG_RUN_ACTIVATED)
    except Exception:
        process_end(False)
        traceback.print_exc()


def setup_environment():
    print(MSG_SETUP_ENVIRONMENT)
    try:
        if not os.path.exists(DOWNLOAD_LOCAL):
            os.mkdir(DOWNLOAD_LOCAL)
        if not os.path.exists(EXTRACTION_LOCAL):
            os.mkdir(EXTRACTION_LOCAL)
        print(MSG_SETUP_ENVIRONMENT_SUCCESS)
        if VERBOSE_MESSAGING:
            print(MSG_VM_TMP_FILES_FOLDER, DOWNLOAD_LOCAL)
            print(MSG_VM_BRANCHES_FILES_FOLDER, EXTRACTION_LOCAL)
    except Exception:
        process_end(False)
        traceback.print_exc()


def database_connect():
    print(MSG_DATABASE_CONNECT)
    try:
        connection = psycopg2.connect(user=USER,
                                      password=PASSWORD,
                                      host=HOST,
                                      port=PORT,
                                      database=DATABASE)
        print(MSG_DATABASE_CONNECT_SUCCESS)
        return connection
    except (Exception, psycopg2.Error):
        if connection:
            connection.close()
        process_end(False)
        traceback.print_exc()


def execute_main_query():
    print(MSG_RUN_MAIN_QUERY)
    global FILE_TYPE_ID
    global FILES_LIMIT
    global MAIN_QUERY
    with CONNECTION.cursor() as cursor:
        ids_list = []
        try:
            cursor.execute(FILE_TYPE_QUERY)
            type_pointer = cursor.fetchone()
            FILE_TYPE_ID = type_pointer[0]

            if FILES_LIMIT != "0":
                MAIN_QUERY += ("limit " + FILES_LIMIT
                               + " offset " + FILES_OFFSET)
            else:
                MAIN_QUERY += " offset " + FILES_OFFSET

            cursor.execute(MAIN_QUERY)
            while True:
                file_pointer = cursor.fetchone()
                if file_pointer is None:
                    break
                ids_list.append(file_pointer[0])
            print(MSG_RUN_MAIN_QUERY_SUCCESS)
            if VERBOSE_MESSAGING:
                print(MSG_VM_TOTAL_FILES_FOUND, len(ids_list))
            print("\n")
            return ids_list
        except Exception:
            process_end(False)
            traceback.print_exc()


def download_extract_file(file_id):
    print(MSG_DOWNLOAD_FILE_NEXT)
    global TOTAL_FILES_DOWNLOADED
    extract_query = (EXTRACT_QUERY + str(file_id))
    with CONNECTION.cursor() as file_cursor:
        file_cursor.execute(extract_query)
        file_content = file_cursor.fetchone()
        with open(DOWNLOAD_LOCAL + file_content[1], "wb") as target_file:
            target_file.write(file_content[0])
        TOTAL_FILES_DOWNLOADED += 1
        print(MSG_DOWNLOAD_FILE_SUCCESS, os.path.basename(target_file.name,))
        return target_file.name


def process_file(target_file_name):
    if(target_file_name.endswith(".zip")):
        process_zip_file(target_file_name)
    else:
        process_text_file(target_file_name)
    print("\n")


def process_zip_file(target_file_name):
    print(MSG_ZIP_FILE_FOUND)
    with ZipFile(target_file_name, "r") as zip_obj:
        file_name_list = zip_obj.namelist()
        zip_obj.extractall(DOWNLOAD_LOCAL)
        for file_name in file_name_list:
            with open(DOWNLOAD_LOCAL + file_name, "r") as unzipped_file:
                process_text_file(unzipped_file.name)
    if DELETE_TMP_FILES_AFTER_PROCESS:
        os.remove(target_file_name)


def process_text_file(target_file_name):
    print(MSG_PROCESS_FILE, os.path.basename(target_file_name))
    global LAST_FILE_SUCCESSFULLY_PARSED
    global TOTAL_FILES_PARSED
    global TOTAL_FILES_PROCESSED
    OFF_COUNTY = False
    RECORD_FLAG = False
    LINES_LIST = []
    TOTAL_COMPANIES_FOUND = 0
    TOTAL_COMPANIES_RECORDED = 0
    NO_BRANCHES_FOUND = True

    with open(target_file_name, "r") as file:
        with open(EXTRACTION_LOCAL + PROCESSED_FILES_PREFIX
                  + os.path.basename(file.name), "w") as branches_file:
            file.seek(0)
            line = file.readline()
            while line:
                if line.startswith("STARTS") or line.startswith("ENDS"):
                    branches_file.write(line)
                elif line.startswith("BLOCKSTARTS"):
                    if TARGET_COD_TOM not in line:
                        LINES_LIST.append(line)
                        OFF_COUNTY = True
                    else:
                        OFF_COUNTY = False
                    TOTAL_COMPANIES_FOUND += 1
                elif line.startswith("COUNTYSTARTS") and OFF_COUNTY:
                    LINES_LIST.append(line)
                    if TARGET_COD_TOM in line:
                        RECORD_FLAG = True
                elif line.startswith("COUNTYENDS") and OFF_COUNTY:
                    LINES_LIST.append(line)
                    if RECORD_FLAG:
                        for record in LINES_LIST:
                            branches_file.write(record)
                        if NO_BRANCHES_FOUND:
                            NO_BRANCHES_FOUND = False
                        TOTAL_COMPANIES_RECORDED += 1
                    OFF_COUNTY = False
                    RECORD_FLAG = False
                    LINES_LIST.clear()
                elif OFF_COUNTY:
                    LINES_LIST.append(line)
                line = file.readline()
    TOTAL_FILES_PARSED += 1
    TOTAL_FILES_PROCESSED += 1
    LAST_FILE_SUCCESSFULLY_PARSED = os.path.basename(
        branches_file.name)
    if DELETE_TMP_FILES_AFTER_PROCESS:
        os.remove(target_file_name)
    if (DELETE_EMPTY_BRANCHES_FILES and os.path.getsize(branches_file.name)
            < FILE_CLEANSE_SIZE):
        os.remove(branches_file.name)
        TOTAL_FILES_PROCESSED -= 1
    print(MSG_PROCESS_FILE_SUCCESS)
    if VERBOSE_MESSAGING:
        print(MSG_VM_TOTAL_COMPANIES_FOUND
              + str(TOTAL_COMPANIES_FOUND))
        if NO_BRANCHES_FOUND:
            print(MSG_VM_NO_COMPANIES_FOUND)
        else:
            print(MSG_VM_TOTAL_COMPANIES_RECORDED
                  + str(TOTAL_COMPANIES_RECORDED))


def cleanup_environment():
    if os.path.exists(DOWNLOAD_LOCAL) and DELETE_TMP_FILES_AFTER_PROCESS:
        os.rmdir(DOWNLOAD_LOCAL)
        if VERBOSE_MESSAGING:
            print(MSG_CLEANUP_ENVIRONMENT)


def long_run_loop(file_id):
    global TOTAL_ERRORS_FOUND_LONG_RUN
    global ERRORS_LIST_LONG_RUN
    global LONG_RUN_ALERT_COUNT
    LONG_RUN_ALERT_COUNT += 1
    if LONG_RUN_ALERT_COUNT >= 50:
        print(MSG_LONG_RUN_ALERT)
        LONG_RUN_ALERT_COUNT = 0
    try:
        target_file_name = download_extract_file(file_id)
        process_file(target_file_name)
    except Exception as error:
        print(MSG_ERROR_LONG_RUN)
        TOTAL_ERRORS_FOUND_LONG_RUN += 1
        ERRORS_LIST_LONG_RUN.append(str(error))
        print(MSG_LONG_RUN_RETRY)
        long_run_loop(file_id)


def normal_loop(file_id):
    try:
        target_file_name = download_extract_file(file_id)
        process_file(target_file_name)
    except Exception:
        process_end(False)
        traceback.print_exc()


def process_end(success):
    if success:
        print(MSG_PROCESS_FINISHED)
    else:
        print(MSG_ERROR_PROCESS_FINISHED)
    if PRINT_REPORT:
        print(MSG_TOTAL_FILES_DOWNLOADED, TOTAL_FILES_DOWNLOADED)
        print(MSG_TOTAL_FILES_PARSED, TOTAL_FILES_PARSED)
        print(MSG_TOTAL_FILES_PROCESSED, TOTAL_FILES_PROCESSED)
        print(MSG_LAST_FILE_SUCCESSFULLY_PARSED,
              LAST_FILE_SUCCESSFULLY_PARSED)
        print("\n")
    if LONG_RUN:
        print(MSG_LONG_RUN_END)
        print(MSG_LONG_RUN_TOTAL_ERRORS, TOTAL_ERRORS_FOUND_LONG_RUN)
        print(MSG_LONGRUN_ERROR_LIST)
        for error_msg in ERRORS_LIST_LONG_RUN:
            print(ID4 + error_msg)


def main():
    print(MSG_SCRIPT_START)
    global CONNECTION
    load_arguments()
    setup_environment()
    with database_connect() as CONNECTION:
        ids_list = execute_main_query()
        if LONG_RUN:
            for file_id in ids_list:
                long_run_loop(file_id)
        else:
            for file_id in ids_list:
                normal_loop(file_id)
    cleanup_environment()
    process_end(True)


# ==============================================
if __name__ == "__main__":
    main()
# ==============================================
