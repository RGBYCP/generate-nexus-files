from collections import OrderedDict
from nexusutils.nexusbuilder import NexusBuilder
from nexusutils.detectorplotter import DetectorPlotter
from nxloghelper import add_example_nxlog
import csv
import numpy as np
import h5py


def __copy_and_truncate(builder, source_dataset_path, target_dataset_path, truncate_to_size):
    """
    Copy data from the existing NeXus file, but truncate datasets to specified length
    """
    source_dataset = builder.source_file.get(source_dataset_path)[:truncate_to_size]
    source_attrs = builder.source_file.get(source_dataset_path).attrs
    if target_dataset_path[0] == "/":
        target_dataset_path = target_dataset_path[0:]
    target_parent_path = "".join(target_dataset_path.split('/')[1:-1])
    target_dataset_name = target_dataset_path.split('/')[-1]
    builder.add_dataset(builder.get_root()[target_parent_path], target_dataset_name, source_dataset,
                        attributes=source_attrs)


def __copy_existing_data():
    """
    Copy data from the existing NeXus file to flesh out the example file
    """
    builder.add_user('Sans2d Team', 'ISIS, STFC')
    event_group_path = nx_entry_name + '/instrument/detector_1/event_data/'
    builder.copy_items(OrderedDict(
        [('raw_data_1/instrument/moderator', nx_entry_name + '/instrument/moderator'),
         ('raw_data_1/instrument/moderator/distance', nx_entry_name + '/instrument/moderator/distance'),
         ('raw_data_1/instrument/source/probe', nx_entry_name + '/instrument/source/probe'),
         ('raw_data_1/instrument/source/type', nx_entry_name + '/instrument/source/type'),
         ('raw_data_1/sample/name', nx_entry_name + '/sample/name'),
         ('raw_data_1/sample/type', nx_entry_name + '/sample/type'),
         ('raw_data_1/duration', nx_entry_name + '/duration'),
         ('raw_data_1/start_time', nx_entry_name + '/start_time'),
         ('raw_data_1/end_time', nx_entry_name + '/end_time'),
         ('raw_data_1/run_cycle', nx_entry_name + '/run_cycle'),
         ('raw_data_1/title', nx_entry_name + '/title'),
         ('raw_data_1/monitor_1/data', nx_entry_name + '/instrument/monitor_1/data'),
         ('raw_data_1/monitor_1/time_of_flight', nx_entry_name + '/instrument/monitor_1/time_of_flight'),
         ('raw_data_1/monitor_2/data', nx_entry_name + '/instrument/monitor_2/data'),
         ('raw_data_1/monitor_2/time_of_flight', nx_entry_name + '/instrument/monitor_2/time_of_flight'),
         ('raw_data_1/monitor_3/data', nx_entry_name + '/instrument/monitor_3/data'),
         ('raw_data_1/monitor_3/time_of_flight', nx_entry_name + '/instrument/monitor_3/time_of_flight'),
         ('raw_data_1/monitor_4/data', nx_entry_name + '/instrument/monitor_4/data'),
         ('raw_data_1/monitor_4/time_of_flight', nx_entry_name + '/instrument/monitor_4/time_of_flight'),
         ('raw_data_1/detector_1_events/', event_group_path),
         #('raw_data_1/detector_1_events/event_id', event_group_path + 'event_id'),
         ('raw_data_1/detector_1_events/event_index', event_group_path + 'event_index'),
         ('raw_data_1/detector_1_events/event_time_zero', event_group_path + 'event_time_zero'),
         ('raw_data_1/detector_1_events/event_time_offset', event_group_path + 'event_time_offset')
         ]))


def __get_spectrum_number_to_detector_id_map(map_filename):
    csv.register_dialect('myDialect',
                         delimiter=' ',
                         skipinitialspace=True)
    with open(map_filename, mode='r') as map_file:
        # Skip the three header lines
        for n in range(3):
            map_file.readline()
        reader = csv.reader(map_file, dialect='myDialect')
        spectrum_to_detector_map = {int(rows[1]): int(rows[0]) for rows in reader}
        return spectrum_to_detector_map


def __convert_spectrum_numbers_to_detector_ids(ids):
    """
    ids are input as spectrum numbers and output as detector IDs (done in place)

    :param ids: numpy array of spectrum numbers
    :return: numpy array of detector IDs
    """
    det_spec_map = __get_spectrum_number_to_detector_id_map('spectrum_gastubes_01.dat')
    for id in np.nditer(ids, op_flags=['readwrite']):
        id[...] = det_spec_map[int(id)]


def __copy_log(builder, source_group, destination_group, nx_component_class=None):
    if nx_component_class is not None:
        split_destination = destination_group.split('/')
        component_name = split_destination[-2]
        log_name = split_destination[-1]
        parent_path_from_entry = '/'.join(split_destination[1:-2])
        component_group = builder.add_nx_group(parent_path_from_entry, component_name, nx_component_class)
        builder.add_nx_group(component_group, log_name, 'NXlog')
    builder.copy_items(OrderedDict(
        [(source_group + '/time', destination_group + '/time'),
         (source_group + '/value', destination_group + '/value')]))


if __name__ == '__main__':
    output_filename = 'SANS2D_ESS_example_2.nxs'
    input_filename = 'SANS2D_ISIS_original.nxs'  # None
    nx_entry_name = 'entry'
    # compress_type=32001 for BLOSC, or don't specify compress_type and opts to get non-compressed datasets
    with NexusBuilder(output_filename, input_nexus_filename=input_filename, nx_entry_name=nx_entry_name,
                      idf_file='SANS2D_Definition_Tubes.xml', compress_type='gzip', compress_opts=1) as builder:
        builder.add_instrument_geometry_from_idf()

        # Define monitor_1 to have the shape of the Utah teapot as example use of NXshape
        builder.add_shape_from_file('../off_files/teapot.off', 'instrument/monitor1', 'shape')

        __copy_existing_data()
        #
        # builder.add_nx_group(builder.get_root(), 'detector_1_events', 'NXevent_data')
        #
        # __copy_and_truncate(builder, 'raw_data_1/detector_1_events/event_id', 'raw_data_1/detector_1_events/event_id',
        #                     7014)
        # __copy_and_truncate(builder, 'raw_data_1/detector_1_events/event_index',
        #                     'raw_data_1/detector_1_events/event_index', 10)
        # __copy_and_truncate(builder, 'raw_data_1/detector_1_events/event_time_zero',
        #                     'raw_data_1/detector_1_events/event_time_zero', 7014)
        # __copy_and_truncate(builder, 'raw_data_1/detector_1_events/event_time_offset',
        #                     'raw_data_1/detector_1_events/event_time_offset', 10)

        add_example_nxlog(builder, '/' + nx_entry_name + '/sample/', 10)

        __copy_log(builder, 'raw_data_1/selog/Guide_Pressure/value_log',
                   nx_entry_name + '/instrument/guide_1/pressure', 'NXguide')

    with h5py.File(input_filename, 'r') as input_file:
        with h5py.File(output_filename, 'r+') as output_file:
            event_id = input_file['raw_data_1/detector_1_events/event_id'][...]
            __convert_spectrum_numbers_to_detector_ids(event_id)
            output_file[nx_entry_name + '/instrument/detector_1/event_data/event_id'] = event_id

    #with DetectorPlotter(output_filename, nx_entry_name) as plotter:
    #    plotter.plot_pixel_positions()
