import math
import os
import pickle
import time

from joblib import Parallel, delayed
import multiprocessing

import pandas as pd

from bs4 import BeautifulSoup


# XML Load ############################################################################################################

def xml2soup(xml_path: str):
    """ Loads xml into BeautifulSoup object.

    Params:
        xml_path (string) - Path to xml to be loaded

    Returns:
        soup (BeautifulSoup object)
    """
    with open(xml_path) as xml:
        soup = BeautifulSoup(xml, 'lxml')
        return soup


# Extractions: Metadata, Counts and Derived Values from Single XML ####################################################

def xml_extract_metadata(xml_soup, output_dict: dict):
    """ Extracts metadata for identification (station, time and location) from top of xml.

    Params:
        output_dict (dict) - Dictionary to add information to
        xml_soup (BeautifulSoup object) - beautifulSoup object containing the loaded xml information.

    Returns:
        output_dict (dict) - Updated dictionary with added metadata information
    """
    identification_elements = xml_soup.find("identification-elements")
    id_element_list = ['date_time', 'tc_identifier', 'station_name', 'station_elevation',
                       'latitude', 'longitude', 'version', 'correction', 'source_uri']
    for id_element in id_element_list:
        try:
            output_dict[id_element] = identification_elements.findChild(name='element',
                                                                        attrs={'name': id_element}
                                                                        ).get("value")
        except AttributeError as error:
            print(error, ". Possibly element '%s' is missing." % id_element)

    # Station identifier are missing in some xml files
    try:
        output_dict['station_identifier'] = identification_elements.findChild(name='element',
                                                                              attrs={'name': 'station_identifier'}
                                                                              ).get("value")
    finally:
        return output_dict


def xml_extract_comparision_value(xml_soup):
    """ Extracts summary statistics from xml (counts of occurrences of all QA flag values).

    These codes only exist in some files and therefore are an indicator of the availability is also added.

    Params:
        xml_soup (BeautifulSoup object) - beautifulSoup object containing the loaded xml information.

    Returns:
        check_stat (dict) - Dictionary with counts for each automatic labeling category
    """
    check_stat = {}

    identification_elements = xml_soup.find("identification-elements")
    if len(identification_elements.findChildren(name='element', attrs={'group': 'qa_summary'})) == 7:
        check_stat["missing_count"] = identification_elements.findChild(name='element',
                                                                        attrs={'name': 'missing_count'}
                                                                        ).get("value")
        check_stat["erroneous_count"] = identification_elements.findChild(name='element',
                                                                          attrs={'name': 'erroneous_count'}
                                                                          ).get("value")
        check_stat["accepted_count"] = identification_elements.findChild(name='element',
                                                                         attrs={'name': 'accepted_count'}
                                                                         ).get("value")
        check_stat["suppressed_count"] = identification_elements.findChild(name='element',
                                                                           attrs={'name': 'suppressed_count'}
                                                                           ).get("value")
        check_stat["doubtful_count"] = identification_elements.findChild(name='element',
                                                                         attrs={'name': 'doubtful_count'}
                                                                         ).get("value")
        check_stat["total_qa_count"] = identification_elements.findChild(name='element',
                                                                         attrs={'name': 'elements_quality_assessed_count'}
                                                                         ).get("value")
        check_stat['missing_qa_summary_stat'] = False
    else:
        check_stat['missing_qa_summary_stat'] = True
    return check_stat


def xml_extract_derived_values(xml_soup, output_dict: dict):
    """Extracts derived variables which are not quality assessed from top of xml.

    Params:
        output_dict (dict) - Dictionary to add information to
        xml_soup (BeautifulSoup object) - beautifulSoup object containing the loaded xml information.

    Returns:
        output_dict (dict) - Updated dictionary with added derived values.
    """

    elements = xml_soup.find("elements").findChildren("element", attrs={"element-index": False, "name": True},
                                                      recursive=False)
    for observation in elements:
        if observation.findChild("qualifier", attrs={"value": "derived"}):
            var_name = observation.get('group') + '-' + observation.get('name') + '-' + observation.get(
                'std-pkg-id') + '-derived'
            output_dict[var_name] = observation.get('value')
        # min/max air temperature time neither are tested nor have a derived value (unit falsely set to datetime)
        elif observation.get('name') in ['minimum_air_temperature_time', 'maximum_air_temperature_time']:
            var_name = observation.get('group') + '-' + observation.get('name') + '-' + observation.get(
                'std-pkg-id') + '-derived'
            output_dict[var_name] = observation.get('value')
        elif observation.get('group') != "qa_summary":
            print("Found unexpected element while parsing for derived values:")
            print(observation.get("name"))
            print(observation)

    return output_dict


# Observations: Extractions from Single Observation in XML ############################################################

def observation_extract_prefix(observation_soup):
    """ Construct unique identify for each transmitted test which can be used as prefix

        Prefix consists of description (name) plus unique identifier (orig-name) as sometimes measurements
        with only few different parameters (like height of sensor, time period) have the same description

        Params:
            observation_soup (BeautifulSoup object) - beautifulSoup object containing single <element> tag from the xml

        Returns:
             prefix (str) - unique identifier for an observation which can be prefixed to identify all related values
    """
    if observation_soup.get("name") == "dummy_bypass_sensor":
        # name="dummy_bypass_sensor" and orig-name="999" is not unique
        # (only exception and not necessarily important but valuable to keep for count check)
        sensor_index = observation_soup.findChild("qualifier", attrs={"name": "sensor_index"}).get("value")
        prefix = observation_soup.get("name") + "_" + observation_soup.get(
            "orig-name") + '_sensor_index_' + sensor_index
    else:
        prefix = observation_soup.get("name") + "_" + observation_soup.get("orig-name")
    return prefix


def observation_extract_native_codes(observation_soup, output_dict: dict, prefix: str):
    """Extracts native error codes send by stations/provider from a given single <element> tag (aka observation)

    Params:
        observation_soup (BeautifulSoup object) - beautifulSoup object containing single <element> tag from the xml
        output_dict (dict) - Dictionary to add information to
        prefix (string) - String to append before all extracted values to define referenced variable.

    Returns:
        output_dict (dict) - Updated dictionary with added key-value pairs for native error codes
    """
    # values related to native error codes send by stations/provider
    output_dict[prefix + "_native-error"] = observation_soup.findChild(name='qualifier',
                                                                       attrs={'name': 'error', 'group': 'quality'},
                                                                       recursive=True).get("value")
    output_dict[prefix + "_native-suspect"] = observation_soup.findChild(name='qualifier',
                                                                         attrs={'name': 'suspect', 'group': 'quality'},
                                                                         recursive=True).get("value")
    output_dict[prefix + "_native-suppressed"] = observation_soup.findChild(name='qualifier',
                                                                            attrs={'name': 'suppressed',
                                                                                   "group": "value"},
                                                                            recursive=True).get("value")
    return output_dict


def observation_extract_qa_category_results(observation_soup, output_dict, category, prefix, output_subtests=True):
    """ Extracts the flag values for a given category of the QA assessment from a single observation.

    Params:
        observation_soup (BeautifulSoup object) - beautifulSoup object containing single <element> tag from the xml
        output_dict (dict) - Dictionary to add information to
        prefix (string) - String to append before all extracted values to define referenced variable.
        output_subtests (boolean) - Indicator if only summary flag (False) or all flag values (True) should be added.

    Returns:
        output_dict (dict) - Updated dictionary with added key-value pairs with QA assessment results for category
    """

    assert category in ['presence', 'range', 'integrity', 'intervariable_comparison', 'temporal']

    obs_category_results = observation_soup.findChild(name='element',
                                                      attrs={'name': category + '_summary', "group": "assessment"},
                                                      recursive=True)
    if obs_category_results:
        output_dict[prefix + "_qa-" + category + '_summary'] = obs_category_results.get("value")
        if output_subtests:
            for subtest in obs_category_results.findChildren(name='element'):
                # Unique identifier for each test is the number in the last level of the test path
                test_path = subtest.get('value')
                test_num = os.path.basename(test_path)
                output_dict[prefix + "_qa-" + category + "_" + test_num] = subtest.findChild(name="qualifier",
                                                                                             attrs={
                                                                                                 "name": "flag_value"}
                                                                                             ).get("value")
    return output_dict


# XML Composition #####################################################################################################

def xml_extraction_loop_observations(output_dict: dict,
                                     xml_soup,
                                     qa_category_list=('presence', 'range', 'integrity', 'intervariable_comparison',
                                                       'temporal'),
                                     output_subtests: bool = True,
                                     native_codes: bool = True,
                                     version='0'):
    """ Extracts for each sensor the values, identifier and test results.

    Values are stored inside of the element tags and only values who implement "element-index" are actually transmitted
    values. Prior to that are metadata information and derived sensor values which are not tested.

    Params:
        output_dict (dict) - Dictionary to add information to
        xml_soup (BeautifulSoup object) - beautifulSoup object containing the loaded xml information.
        qa_category_list (list) - List of all QA categories whose flag values should be extracted
        output_subtests (boolean) - Indicator if only summary flag (False) or all flag values (True) should be added.
        native_codes (boolean) - Indicator if natives codes are to be extracted
        version (str) - String with the version number from the XML

    Returns:
        output_dict (dict) - Updated dictionary with added key-value pairs for each sensor
    """
    assert all(cat in ['presence', 'range', 'integrity', 'intervariable_comparison', 'temporal']
               for cat in qa_category_list)

    elements = xml_soup.find("elements").findChildren("element", attrs={"element-index": True,
                                                                        "name": True,
                                                                        "orig-name": True}, recursive=False)

    for observation in elements:
        if (version == '0') or (observation.findChild("status-indicators", recursive=True)):
            prefix = observation_extract_prefix(observation)
            if observation.has_attr('orig-value'):
                output_dict[prefix + "_orig-value"] = observation.get("orig-value")
            if observation.has_attr('value'):
                output_dict[prefix + "_value"] = observation.get("value")

            output_dict[prefix + "_overall_qa_summary"] = observation.findChild(name='element',
                                                                                attrs={'name': 'overall_qa_summary'},
                                                                                recursive=True).get("value")

            for qa_category in qa_category_list:
                output_dict = observation_extract_qa_category_results(observation_soup=observation,
                                                                      output_dict=output_dict,
                                                                      category=qa_category,
                                                                      prefix=prefix,
                                                                      output_subtests=output_subtests)

            if native_codes:
                output_dict = observation_extract_native_codes(observation,
                                                               output_dict,
                                                               prefix=prefix)

            if version != '0':
                status_indicator = observation.findChild("status-indicators", recursive=True)
                if status_indicator.findChild("element", attrs={"name": "qa_flag_override"}):
                    output_dict[prefix + "_qa_flag_override"] = status_indicator.findChild("element",
                                                                                           attrs={"name": "qa_flag_override"}
                                                                                           ).get("value")
                elif status_indicator.findChild("element", attrs={"name": "value_override"}):
                    output_dict[prefix + "_value_override"] = status_indicator.findChild("element",
                                                                                         attrs={"name": "value_override"}
                                                                                         ).get("value")
                output_dict[prefix + "_qc_remark"] = status_indicator.findChild("element",
                                                                                attrs={"name": "qc_remark"}
                                                                                ).get("value")

    return output_dict


def xml_extraction_complete_compose(xml_path: str,
                                    qa_category_list=('presence', 'range', 'integrity', 'intervariable_comparison',
                                                      'temporal'),
                                    output_subtests: bool = True,
                                    native_codes: bool = True):
    """ Composes an unified extraction dictionary per xml containing the meta data, derived values and
    measurements/tests as well as the original filename for verification.
    """
    station_data = {}
    soup = xml2soup(xml_path)
    station_data = xml_extract_metadata(xml_soup=soup, output_dict=station_data)

    # Only the original versions contain the derived values.
    if station_data['version'] == '0':
        station_data = xml_extract_derived_values(xml_soup=soup,
                                                  output_dict=station_data)
    station_data = xml_extraction_loop_observations(output_dict=station_data,
                                                    qa_category_list=qa_category_list,
                                                    xml_soup=soup,
                                                    output_subtests=output_subtests,
                                                    native_codes=native_codes,
                                                    version=station_data['version'])
    # For debugging purposes we will add the source information
    station_data['origin_filename'] = xml_path
    return station_data


# Sanity Check #####################################################################################################

def compare_to_count_values(xml_dict: dict, compare_dict: dict):
    """ Sanity check if the extracted values equal the summary count values provided in the XML."""
    if compare_dict['missing_qa_summary_stat']:
        print("No comparision values for test in XML.")
    else:
        df_xml = pd.DataFrame({"xml": xml_dict})
        index = [i for i in df_xml.index if i.endswith('overall_qa_summary')]
        df_xml = df_xml.loc[index]
        xml_counts = df_xml['xml'].value_counts()

        flag_meaning = {'-1': 'missing_count',
                        '0': 'erroneous_count',
                        '100': 'accepted_count',
                        '-10': 'suppressed_count',
                        '10': 'doubtful_count',
                        'total': 'total_qa_count'}

        xml_counts.index = xml_counts.reset_index()['index'].replace(flag_meaning).values
        xml_counts = pd.DataFrame(xml_counts)
        assert xml_counts.xml.sum() == int(compare_dict['total_qa_count']), print(
            'Extracted %d values from xml, expected %d' % (
                xml_counts.xml.sum(), int(compare_dict['total_qa_count'])))
        df_check = pd.DataFrame({"check_sum": compare_dict})
        df_check.drop('total_qa_count', inplace=True)
        df_check.drop('missing_qa_summary_stat', inplace=True)
        df_check = df_check.merge(xml_counts, how='left', left_index=True, right_index=True).fillna(0)
        df_check['valid'] = (df_check.check_sum.astype(int) == df_check.xml)
        assert df_check.valid.all(), print('Some categories have an unexpected number of values extracted \n', df_check)
        print("All value counts for flag categories match the value count in the extraction for file.",
              xml_dict["station_name"],
              xml_dict["date_time"])


# Helper functions #####################################################################################################

def save_pickle(folder_path: str, file_name: str, save_object):
    """Helper function to save an object to a pickle file """
    with open(folder_path + file_name + '.pickle', 'wb') as handle:
        pickle.dump(save_object, handle, protocol=pickle.HIGHEST_PROTOCOL)


# Main usage for the inversion of the mapping between filename and station to get for each station all file names at once.
def inverse_dict(input_dict: dict):
    """ Inverses the first level key-value relationships of a dict.
    The resulting dictionary gives for each value (of the original dict) the set of keys which pointed towards it.
    """
    inv_map = {}
    for key, value in input_dict.items():
        inv_map.setdefault(value, set()).add(key)
    return inv_map


def ensure_folder_exists(folder_path: str):
    """" Check whether folder exists and if not create folder. """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


# Used to add complete path to file names for easy access.
def append_prefix_to_mapping_values(mapping_dict: dict, prefix: str):
    """ Updates all dictionary values with a prefix. """
    for name_key, file_list in mapping_dict.items():
        mapping_dict[name_key] = {prefix + file_name for file_name in file_list}
    return mapping_dict


def list_values_containing_substring(columns_list: list, sub_string: str, sensor_value_only: bool = False):
    columns_list = [key for key in columns_list if sub_string in key]
    if sensor_value_only:
        columns_list = [key for key in columns_list if ('_value' in key)]
    return columns_list


def dict_keys_containing_substring(xml_dict: dict, sub_string: str, sensor_value_only: bool = False):
    columns_list = list(xml_dict.keys())
    columns_list = list_values_containing_substring(columns_list=columns_list,
                                                    sub_string=sub_string,
                                                    sensor_value_only=sensor_value_only)
    return columns_list


# Chunking Methods ####################################################################################################

def create_mapping_filename2station(folder: list, folder_path: str, multi_process: bool):
    """ Creates a dictionary with the xml filename as key and station as a value. This enables the chunking of the data
    by station instead of randomly processing a certain amount of files.
    """
    result_dict = {}
    if not multi_process:
        for file in folder:
            soup = xml2soup(folder_path+file)
            id_elements = soup.find("identification-elements")
            source_uri = id_elements.findChild(name='element', attrs={'name': "source_uri"}).get("value")
            unique_file_id = "_".join(source_uri.split(sep="/")[10:8:-1])
            result_dict[file] = unique_file_id
    elif multi_process:
        chunks_dict = list_chunking(folder)
        dict_iterable = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(create_mapping_filename2station
                                                                             )(folder=folder_chunk,
                                                                               folder_path=folder_path,
                                                                               multi_process=False)
                                                                     for _, folder_chunk in chunks_dict.items())
        for chunk_result in dict_iterable:
            result_dict.update(chunk_result)
    return result_dict


def get_mapping_filename2station(folder_path: str, multi_process: bool = True):
    """ Returns a dict with mappings between file names and stations by either loading an existing pickle file
    or by running through all files (and leaving a pickle file for future iterations)

    Params:
        folder_path (str) - path to the folder with XML files

    Returns:
        result_dict (dict) - Dictionary mapping each filename (key) to a station (value) the information comes from

    Conditional Byproduct:
        mapping_filename2station (pickle file) - Pickled version of result_dict saved to folder if not already existent
    """
    try:
        with open(folder_path + 'mapping_filename2station.pickle', 'rb') as pickle_file:
            result_dict = pickle.load(pickle_file)
        print("--- Loaded existing mapping between file names and stations.---")
    except FileNotFoundError:
        folder = os.listdir(folder_path)
        # TODO for Prod: Replace length check (>38) with real verification of XML without reading all files
        folder = [file for file in folder if len(file) > 38]
        result_dict = create_mapping_filename2station(folder=folder,
                                                      folder_path=folder_path,
                                                      multi_process=multi_process)

        save_pickle(folder_path=folder_path,
                    file_name='mapping_filename2station',
                    save_object=result_dict)
        print("--- Created and saved mapping between file names and stations.---")
    return result_dict


def get_mapping_station2filename(folder_path: str):
    """ Returns dictionary with a set of file names (value) in the folder which belong to one station (key).
    If the folder doesn't already contain a pickle file with the opposite mapping then this is created first.
    """
    mapping_dict = get_mapping_filename2station(folder_path)
    inverse_mapping_dict = inverse_dict(mapping_dict)
    return inverse_mapping_dict


def list_chunking(input_list: list, chunksize: int = None):
    """Transform list to a dict of lists which contain successive chunks of length chunksize from the original list."""
    if not chunksize:
        default_max_chunksize = 1000
        n_cpu = multiprocessing.cpu_count()
        fair_cpu_divide = int(math.ceil(len(input_list) / n_cpu))
        chunksize = min(fair_cpu_divide, default_max_chunksize)

    output_dict = {}
    for start_item in range(0, len(input_list), chunksize):
        end_item = min(start_item + chunksize, len(input_list))
        output_dict[str(start_item) + '_to_' + str(end_item)] = input_list[start_item:end_item]
    return output_dict


# End2End-Methods ####################################################################################################

def xml_list_to_pickled_extraction_dict(input_files: list, output_path: str):
    """Extract all potentially interesting values from a list of xml files and combine them to a single dict """
    result_dict = {}
    for file in input_files:
        station_data = xml_extraction_complete_compose(file, output_subtests=True, native_codes=True)
        source_uri = station_data['source_uri']
        unique_file_id = "_".join(source_uri.split(sep="/")[10:7:-1]) + '_' + source_uri.split(sep="/")[12]
        result_dict[unique_file_id] = station_data

    file_name = os.path.basename(output_path)
    folder_path = output_path.replace(file_name, '')
    save_pickle(folder_path=folder_path, file_name=file_name, save_object=result_dict)


def xml_folder_to_pickled_extraction_dicts(input_folder_path: str,
                                           chunk_by: str,
                                           chunksize: int = 1000,
                                           file_chunks_dict: dict = None,
                                           multi_process: bool = True):
    """ End-to-end function for the complete extraction process from input folder (with xml's) to output folder with
    dictionaries of extracted values.

     Params:
        input_folder_path (str) - Path of the folder containing (only) the xml files.
        chunk_by ("chunksize"/"station") - Indicator how to break the data into chunks for processing.
                                           "chunksize" = fixed number of xml files per chunk
                                           "station" = all xml from the same station at once
        chunksize (int) - Number of xml files per chunk (only considered if chunk_by="chunksize")
        file_chunks_dict (dict) - Dictionary of which files should be processed together. Is ignored for
                                  chunk_by='chunksize' and if not given will be calculated for chunk_by='station'.
        multi_process (bool) - Indicator if single or all available kernels should be used for processing.

    Returns:
        Nothing returned, but creates dictionaries in the data/interim folder (same subsequent structure as input path)
        with dictionaries with the extracted values split depending on the chunk_by parameter.
    """

    assert chunk_by in ["chunksize", "station"]

    if chunk_by == "chunksize" and file_chunks_dict:
        print("Argument file_chunks_dict will be ignored for chunk_by='chunksize'")

    folder = os.listdir(input_folder_path)
    output_path = input_folder_path.replace('raw', 'interim')

    if folder:
        ensure_folder_exists(output_path)
        folder = [input_folder_path + file for file in folder if len(file) > 38]
    else:
        print("Nothing to do. Folder doesn't contain files with length over 38 characters.")

    if chunk_by == 'station' and not file_chunks_dict:
        file_chunks_dict = get_mapping_station2filename(folder_path=input_folder_path)
        file_chunks_dict = append_prefix_to_mapping_values(mapping_dict=file_chunks_dict, prefix=input_folder_path)
    elif chunk_by == "chunksize":
        file_chunks_dict = list_chunking(input_list=folder, chunksize=chunksize)

    if multi_process:
        Parallel(n_jobs=multiprocessing.cpu_count())(
            delayed(xml_list_to_pickled_extraction_dict)(file_list, output_path+name_key)
            for name_key, file_list in file_chunks_dict.items())

    elif not multi_process:
        for (name_key, file_list) in file_chunks_dict.items():
            xml_list_to_pickled_extraction_dict(input_files=file_list, output_path=output_path+name_key)


if __name__ == "__main__":
    # execute only if run as a script
    path = "../../data/raw/eccc_ml_qa/all_stations_2019/deploy/"
    start_time = time.time()
    xml_folder_to_pickled_extraction_dicts(input_folder_path=path, chunk_by='station', multi_process=True)
    print("--- %s second ---" % (time.time() - start_time))
