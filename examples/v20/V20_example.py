from collections import OrderedDict
from nexusutils.nexusbuilder import NexusBuilder
import numpy as np
import nexusformat.nexus as nexus
from nexusjson.nexus_to_json import NexusToDictConverter, create_writer_commands, object_to_json_file
from datetime import datetime
from typing import List

"""
The origin of the coordinate system is the position of the beam at the end of the neutron guide,
47m from the cold source, as described in Woracek et al 2016
"""


def __copy_and_transform_dataset(source_file, source_path, target_path, transformation=None, dtype=None):
    source_data = source_file[source_path][...]
    if transformation is not None:
        transformed_data = transformation(source_data)
    else:
        transformed_data = source_data
    if dtype is None:
        dtype = transformed_data.dtype
    target_dataset = builder.target_file.create_dataset(target_path, transformed_data.shape,
                                                        dtype=dtype,
                                                        compression=builder.compress_type,
                                                        compression_opts=builder.compress_opts)
    target_dataset[...] = transformed_data
    return target_dataset


def __copy_existing_data(downscale_detecter=False):
    """
    Copy data from the existing NeXus file
    """
    raw_event_path = nx_entry_name + '/instrument/detector_1/raw_event_data/'
    builder.get_root()['instrument/detector_1'].create_group('raw_event_data')
    builder.copy_items(OrderedDict(
        [('entry-01/Delayline_events/event_time_offset', raw_event_path + 'event_time_offset')
         ]))

    __copy_and_transform_dataset(builder.source_file, 'entry-01/Delayline_events/event_index',
                                 raw_event_path + 'event_index', dtype=np.uint64)

    def shift_time(timestamps):
        first_timestamp = 59120017391465
        new_start_time = 1543584772000000000
        return timestamps - first_timestamp + new_start_time

    event_time_zero_ds = __copy_and_transform_dataset(builder.source_file, 'entry-01/Delayline_events/event_time_zero',
                                                      raw_event_path + 'event_time_zero', shift_time)
    event_time_zero_ds.attrs.create('units', np.array('ns').astype('|S2'))
    event_time_zero_ds.attrs.create('offset', np.array('1970-01-01T00:00:00').astype('|S19'))

    def downscale_detector_resolution(ids):
        original_res = (2 ** 16) ** 2
        target_res = 150 ** 2
        scale_factor = target_res / original_res
        return (ids * scale_factor).astype(np.uint32)

    if downscale_detecter:
        __copy_and_transform_dataset(builder.source_file, 'entry-01/Delayline_events/event_id',
                                     raw_event_path + 'event_id', downscale_detector_resolution)
    else:
        __copy_and_transform_dataset(builder.source_file, 'entry-01/Delayline_events/event_id',
                                     raw_event_path + 'event_id')


def __copy_log(builder, source_group, destination_group, nx_component_class=None):
    split_destination = destination_group.split('/')
    log_name = split_destination[-1]
    if nx_component_class is not None:
        component_name = split_destination[-2]
        parent_path_from_entry = '/'.join(split_destination[1:-2])
        component_group = builder.add_nx_group(parent_path_from_entry, component_name, nx_component_class)
        builder.add_nx_group(component_group, log_name, 'NXlog')
    else:
        builder.add_nx_group('/'.join(split_destination[1:-1]), log_name, 'NXlog')
    builder.copy_items(OrderedDict(
        [(source_group + '/time', destination_group + '/time'),
         (source_group + '/value', destination_group + '/value')]))


def __add_chopper(builder, number):
    chopper_group = builder.add_nx_group(instrument_group, 'chopper_' + str(number), 'NXdisk_chopper')

    # TDC position is unknown for the Airbus choppers, due to them being refurbished two times
    if number is 1:
        builder.add_dataset(chopper_group, 'name', 'Airbus, Source Chopper, ESS Pulse, Disc 1')
        builder.add_dataset(chopper_group, 'slit_edges', [0., 23.], attributes={'units': 'deg'})
        builder.add_dataset(chopper_group, 'slits', 1)
        builder.add_dataset(chopper_group, 'slit_height', 150., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'radius', 350., attributes={'units': 'mm'})
        distance_from_end_of_guide = -25.3
        record_z_position(builder, chopper_group, distance_from_end_of_guide)
    elif number is 2:
        builder.add_dataset(chopper_group, 'name', 'Airbus, Source Chopper, ESS Pulse, Disc 2')
        builder.add_dataset(chopper_group, 'slit_edges', [0., 50.], attributes={'units': 'deg'})
        # Actually has 2 slits, but only one is used and I don't have dimensions for the second slit
        builder.add_dataset(chopper_group, 'slits', 1)
        builder.add_dataset(chopper_group, 'slit_height', 150., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'radius', 350., attributes={'units': 'mm'})
        distance_from_end_of_guide = -25.3
        record_z_position(builder, chopper_group, distance_from_end_of_guide)
    elif number is 3:
        builder.add_dataset(chopper_group, 'name', 'Juelich, WFM Chopper, Disc 1')
        builder.add_dataset(chopper_group, 'slit_edges', np.array(
            [0., 83.71, 94.7, 140.49, 155.79, 193.26, 212.56, 242.32, 265.33, 287.91, 314.37, 330.3]) + 15.0,
                            attributes={'units': 'deg'})
        builder.add_dataset(chopper_group, 'slits', 6)
        builder.add_dataset(chopper_group, 'slit_height', 130., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'radius', 300., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'ntp_to_mrf_comparison', 0)
        distance_from_end_of_guide = -18.45
        record_z_position(builder, chopper_group, distance_from_end_of_guide)
    elif number is 4:
        builder.add_dataset(chopper_group, 'name', 'Juelich, WFM Chopper, Disc 2')
        builder.add_dataset(chopper_group, 'slit_edges', np.array(
            [0., 65.04, 76.03, 126.1, 141.4, 182.88, 202.18, 235.67, 254.97, 284.73, 307.74, 330.0]) + 15.0,
                            attributes={'units': 'deg'})
        builder.add_dataset(chopper_group, 'slits', 6)
        builder.add_dataset(chopper_group, 'slit_height', 130., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'radius', 300., attributes={'units': 'mm'})
        distance_from_end_of_guide = -18.45
        record_z_position(builder, chopper_group, distance_from_end_of_guide)
    elif number is 5:
        builder.add_dataset(chopper_group, 'name', 'Juelich, Frame Overlap Chopper, Disc 1')
        builder.add_dataset(chopper_group, 'slit_height', 130., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'radius', 300., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'slit_edges', np.array(
            [0., 64.35, 84.99, 125.05, 148.29, 183.41, 205.22, 236.4, 254.27, 287.04, 302.8, 335.53]) + 15.0,
                            attributes={'units': 'deg'})
        builder.add_dataset(chopper_group, 'slits', 6)
        distance_from_end_of_guide = -16.5
        record_z_position(builder, chopper_group, distance_from_end_of_guide)
    elif number is 6:
        builder.add_dataset(chopper_group, 'name', 'Airbus, Wavelength-Band Chopper, Disc 1')
        builder.add_dataset(chopper_group, 'pair_separation', 24.2, attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'slit_edges', [0., 140.], attributes={'units': 'deg'})
        builder.add_dataset(chopper_group, 'slits', 1)
        builder.add_dataset(chopper_group, 'slit_height', 150., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'radius', 350., attributes={'units': 'mm'})
        distance_from_end_of_guide = -15.3
        record_z_position(builder, chopper_group, distance_from_end_of_guide)
    elif number is 7:
        builder.add_dataset(chopper_group, 'name', 'Airbus, Wavelength-Band Chopper, Disc 2')
        builder.add_dataset(chopper_group, 'pair_separation', 24.2, attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'slit_edges', [0., 202.], attributes={'units': 'deg'})
        builder.add_dataset(chopper_group, 'slits', 1)
        builder.add_dataset(chopper_group, 'slit_height', 150., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'radius', 350., attributes={'units': 'mm'})
        distance_from_end_of_guide = -15.3
        record_z_position(builder, chopper_group, distance_from_end_of_guide)
    elif number is 8:
        builder.add_dataset(chopper_group, 'name', 'Juelich, Frame Overlap Chopper, Disc 2')
        builder.add_dataset(chopper_group, 'slit_height', 130., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'radius', 300., attributes={'units': 'mm'})
        builder.add_dataset(chopper_group, 'slit_edges', np.array(
            [0., 79.78, 116.38, 136.41, 172.47, 191.73, 221.94, 240.81, 267.69, 287.13, 311.69, 330.89]) + 15.0,
                            attributes={'units': 'deg'})
        builder.add_dataset(chopper_group, 'slits', 6)
        distance_from_end_of_guide = -9.4
        record_z_position(builder, chopper_group, distance_from_end_of_guide)

    chopper_group.create_group('top_dead_center')
    builder.add_feature("B89B086951FEFDDF")


def record_z_position(builder, parent_group, distance_from_end_of_guide: float):
    transforms = builder.add_nx_group(parent_group, 'transformations', 'NXtransformations')
    position = builder.add_transformation(transforms, 'translation', distance_from_end_of_guide, 'm', [0., 0., 1.],
                                          name='position')
    builder.add_dataset(parent_group, 'depends_on', position.name)


def __add_choppers(builder):
    for chopper_number in range(1, 9):
        __add_chopper(builder, chopper_number)


def __add_detector(builder):
    """
    Description of V20's DENEX (delay line) detector
    :param builder:
    :return:
    """

    pixels_per_axis = 512
    detector_width = 0.28
    pixel_size = detector_width / pixels_per_axis
    half_detector_width = detector_width / 2.0
    half_pixel_width = pixel_size / 2.0
    single_axis_offsets = (pixel_size * np.arange(0, pixels_per_axis, 1,
                                                  dtype=np.float)) - half_detector_width + half_pixel_width
    detector_group = builder.add_nx_group(builder.get_root()['instrument'], 'detector_1', 'NXdetector')
    x_offsets, y_offsets = np.meshgrid(single_axis_offsets,
                                       single_axis_offsets)
    builder.add_dataset(detector_group, 'x_pixel_offset', x_offsets, {'units': 'm'})
    builder.add_dataset(detector_group, 'y_pixel_offset', y_offsets, {'units': 'm'})

    builder.add_dataset(detector_group, 'local_name', 'DENEX delay line detector')

    pixel_shape = builder.add_nx_group(detector_group, 'pixel_shape', 'NXoff_geometry')
    pixel_verts = np.array([[-0.001, -0.001, 0.0], [0.001, -0.001, 0.0], [0.001, 0.001, 0.0], [-0.001, 0.001, 0.0]],
                           dtype=np.float32)
    pixel_winding_order = np.array([0, 1, 2, 3], dtype=np.int32)
    pixel_faces = np.array([0], dtype=np.int32)
    builder.add_dataset(pixel_shape, 'faces', pixel_faces)
    builder.add_dataset(pixel_shape, 'vertices', pixel_verts, {'units': 'm'})
    builder.add_dataset(pixel_shape, 'winding_order', pixel_winding_order)

    pixel_ids = np.arange(0, pixels_per_axis ** 2, 1, dtype=int)
    pixel_ids = np.reshape(pixel_ids, (pixels_per_axis, pixels_per_axis))
    builder.add_dataset(detector_group, 'detector_number', pixel_ids)

    # builder.add_shape(detector_group, 'detector_shape', vertices, faces, detector_faces.T)
    # Add detector position
    transforms = builder.add_nx_group(detector_group, 'transformations', 'NXtransformations')
    orientation = builder.add_transformation(transforms, 'rotation', [90.0], 'deg', [0.0, 1.0, 0.0], name='orientation',
                                             depends_on='.')
    z_offset = builder.add_transformation(transforms, 'translation', [3.6], 'm', [0.0, 0.0, 1.0],
                                          name='beam_direction_offset', depends_on=orientation.name)
    x_offset = builder.add_transformation(transforms, 'translation', [0.935], 'm', [1.0, 0.0, 0.0], name='location',
                                          depends_on=z_offset.name)
    builder.add_dataset(detector_group, 'depends_on', x_offset.name)

    # Placeholders for streamed data
    for channel_number in range(4):
        detector_group.create_group(f'waveforms_channel_{channel_number}')
        detector_group.create_group(f'pulses_channel_{channel_number}')

    for hv_power_supply_channel in range(4):
        detector_group.create_group(f'hv_supply_voltage_channel_{hv_power_supply_channel + 1}')
        detector_group.create_group(f'hv_supply_current_channel_{hv_power_supply_channel + 1}')
        detector_group.create_group(f'hv_supply_status_channel_{hv_power_supply_channel + 1}')

    __add_readout_system(builder, detector_group)

    # builder.add_nx_group(builder.get_root(), 'raw_event_data', 'NXevent_data')


def __add_monitors(builder):
    """
    Helium-3 monitors
    :param builder:
    :return:
    """
    distance_from_guide = 0.18
    monitor_group_1 = builder.add_nx_group(builder.get_root(), 'monitor_1', 'NXmonitor')
    monitor_group_1.create_group('events')
    builder.add_dataset(monitor_group_1, 'detector_number', 262144)
    record_z_position(builder, monitor_group_1, distance_from_guide)
    builder.add_dataset(monitor_group_1, 'name', 'Helium-3 monitor 1')

    distance_from_guide = 3.35
    monitor_group_2 = builder.add_nx_group(builder.get_root(), 'monitor_2', 'NXmonitor')
    monitor_group_2.create_group('events')
    builder.add_dataset(monitor_group_2, 'detector_number', 262145)
    record_z_position(builder, monitor_group_2, distance_from_guide)
    builder.add_dataset(monitor_group_2, 'name', 'Helium-3 monitor 2')


def __add_readout_system(builder, parent_group):
    for readout_system_number in ('1', '2'):
        group_name = f'readout_system_{readout_system_number}'
        readout_group = parent_group.create_group(group_name)
        readout_group.create_group('s_diff')
        readout_group.create_group('n_diff')
        readout_group.create_group('status')


def __add_motion_devices(builder):
    def _add_motion(builder, group_names: List[str], start_number: int = 0, nx_class: str = 'NXpositioner',
                    pv_root: str = None, value_name: str = "value", distance_from_guide: float = 0.0):
        for group_number, group_name in enumerate(group_names):
            try:
                group = builder.add_nx_group(builder.get_root()['instrument'], group_name, nx_class)
                record_z_position(builder, group, distance_from_guide)
            except ValueError:
                # If the group already exists that's fine
                group = builder.get_root()['instrument'][group_name]
            group.create_group(f'{value_name}_target')
            group.create_group(f'{value_name}')
            group.create_group(f'{value_name}_status')
            group.create_group(f'{value_name}_velocity')
            if pv_root is not None:
                builder.add_dataset(group, 'controller_record', pv_root.format(group_number + start_number))

    _add_motion(builder, ['linear_stage', 'tilting_angle_1', 'tilting_angle_2'], 1, pv_root='TUD-SMI:MC-MCU-01:m{}.VAL')
    _add_motion(builder, ['Omega_1', 'Omega_2', 'Lin1'], 10, pv_root='HZB-V20:MC-MCU-01:m{}.VAL')
    _add_motion(builder, ['Slit3'], nx_class='NXslit', value_name='x_gap', distance_from_guide=3.275)
    _add_motion(builder, ['Slit3'], nx_class='NXslit', value_name='y_gap')
    _add_motion(builder, ['Slit3'], nx_class='NXslit', value_name='x_center')
    _add_motion(builder, ['Slit3'], nx_class='NXslit', value_name='y_center')
    builder.get_root()['instrument']['Slit3'].create_group('x_gap_from_nicos_cache')
    builder.get_root()['instrument']['Slit3'].create_group('y_gap_from_nicos_cache')
    builder.get_root()['instrument']['Slit3'].create_group('x_center_from_nicos_cache')
    builder.get_root()['instrument']['Slit3'].create_group('y_center_from_nicos_cache')

    slit2_group = builder.add_nx_group(builder.get_root()['instrument'], 'slit2', 'NXslit')
    record_z_position(builder, slit2_group, 0.08)


def __create_file_writer_command(filepath):
    streams = {}

    # DENEX detector
    detector_topic = 'denex_detector'
    __add_data_stream(streams, detector_topic, 'delay_line_detector',
                      '/entry/instrument/detector_1/raw_event_data', 'ev42')
    detector_debug_topic = 'denex_debug'
    for detector_channel in range(4):
        __add_data_stream(streams, detector_debug_topic, f'Denex_Adc0_Ch{detector_channel}',
                          f'/entry/instrument/detector_1/pulses_channel_{detector_channel}', 'ev42')
        __add_data_stream(streams, detector_debug_topic, f'Denex_Adc0_Ch{detector_channel}_waveform',
                          f'/entry/instrument/detector_1/waveforms_channel_{detector_channel}', 'senv')

    # Detector HV supply
    hv_supply_topic = 'V20_detectorPower'
    for hv_power_supply_channel in range(4):
        __add_data_stream(streams, hv_supply_topic, f'HZB-V20:Det-PwrC-01:02:00{hv_power_supply_channel}:VMon',
                          f'/entry/instrument/detector_1/hv_supply_voltage_channel_{hv_power_supply_channel + 1}',
                          'f142', 'double')
        __add_data_stream(streams, hv_supply_topic, f'HZB-V20:Det-PwrC-01:02:00{hv_power_supply_channel}:IMon',
                          f'/entry/instrument/detector_1/hv_supply_current_channel_{hv_power_supply_channel + 1}',
                          'f142', 'double')
        __add_data_stream(streams, hv_supply_topic, f'HZB-V20:Det-PwrC-01:02:00{hv_power_supply_channel}:Pw',
                          f'/entry/instrument/detector_1/hv_supply_status_channel_{hv_power_supply_channel + 1}',
                          'f142', 'int32')

    # Monitors
    monitor_topic = 'monitor'
    # ch2 is monitor 1 which is upstream of ch1, monitor 2
    __add_data_stream(streams, monitor_topic, 'Monitor_Adc0_Ch2',
                      '/entry/monitor_1/events', 'ev42')
    __add_data_stream(streams, monitor_topic, 'Monitor_Adc0_Ch1',
                      '/entry/monitor_2/events', 'ev42')

    # Choppers
    chopper_topic = 'V20_choppers'
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0401:TDC_array',
                      '/entry/instrument/chopper_1/top_dead_center', 'tdct')
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0402:TDC_array',
                      '/entry/instrument/chopper_2/top_dead_center', 'tdct')
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0101:TDC_array',
                      '/entry/instrument/chopper_3/top_dead_center', 'tdct')
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0102:TDC_array',
                      '/entry/instrument/chopper_4/top_dead_center', 'tdct')
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0301:TDC_array',
                      '/entry/instrument/chopper_5/top_dead_center', 'tdct')
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0501:TDC_array',
                      '/entry/instrument/chopper_6/top_dead_center', 'tdct')
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0502:TDC_array',
                      '/entry/instrument/chopper_7/top_dead_center', 'tdct')
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0302:TDC_array',
                      '/entry/instrument/chopper_8/top_dead_center', 'tdct')
    __add_data_stream(streams, chopper_topic, 'HZB-V20:Chop-Drv-0101:Ref_Unix_asub.VALF',
                      '/entry/instrument/chopper_3/ntp_to_mrf_comparison', 'f142', 'int32')

    # Readout system timing status
    timing_status_topic = 'V20_timingStatus'
    for readout_system_number in ('1', '2'):
        group_name = f'readout_system_{readout_system_number}'
        __add_data_stream(streams, timing_status_topic, f'HZB-V20:TS-RO{readout_system_number}:TS-SDiff-RBV',
                          f'/entry/instrument/detector_1/{group_name}/s_diff', 'f142', 'double')
        __add_data_stream(streams, timing_status_topic, f'HZB-V20:TS-RO{readout_system_number}:TS-NDiff-RBV',
                          f'/entry/instrument/detector_1/{group_name}/n_diff', 'f142', 'double')
        __add_data_stream(streams, timing_status_topic, f'HZB-V20:TS-RO{readout_system_number}:STATUS2-RBV',
                          f'/entry/instrument/detector_1/{group_name}/status', 'f142', 'int32')

    # Motion devices
    def _add_motion_dev(pv_root: str, group_names: List[str], start_index: int):
        motion_topic = 'V20_motion'
        for group_number, group_name in enumerate(group_names):
            __add_data_stream(streams, motion_topic, pv_root + f"{group_number + start_index}.VAL",
                              f'/entry/instrument/{group_name}/target_value', 'f142', 'double')
            __add_data_stream(streams, motion_topic, pv_root + f"{group_number + start_index}.RBV",
                              f'/entry/instrument/{group_name}/value', 'f142', 'double')
            __add_data_stream(streams, motion_topic, pv_root + f"{group_number + start_index}.STAT",
                              f'/entry/instrument/{group_name}/status', 'f142', 'int32')
            __add_data_stream(streams, motion_topic, pv_root + f"{group_number + start_index}.VELO",
                              f'/entry/instrument/{group_name}/velocity', 'f142', 'double')

    motion_topic = 'V20_motion'
    _add_motion_dev("TUD-SMI:MC-MCU-01:m", ['linear_stage', 'tilting_angle_1', 'tilting_angle_2'], start_index=1)
    _add_motion_dev("HZB-V20:MC-MCU-01:m", ['Omega_1', 'Omega_2', 'Lin1'], start_index=10)

    def _add_slit(slit_group_name: str, pv_names: List[str]):
        nicos_device_name = slit_group_name.lower()
        nicos_topic = "V20_nicosCacheHistory"
        for pv_name in pv_names:
            if "H-Gap" in pv_name:
                value_name = "x_gap"
                nicos_value_name = "h_gap"
            elif "V-Gap" in pv_name:
                value_name = "y_gap"
                nicos_value_name = "v_gap"
            elif "H-Center" in pv_name:
                value_name = "x_center"
                nicos_value_name = "h_center"
            elif "V-Center" in pv_name:
                value_name = "y_center"
                nicos_value_name = "v_center"
            else:
                value_name = "value"
                nicos_value_name = "value"
            __add_data_stream(streams, motion_topic, f"{pv_name}.VAL",
                              f'/entry/instrument/{slit_group_name}/{value_name}_target', 'f142', 'double')
            __add_data_stream(streams, motion_topic, f"{pv_name}.RBV",
                              f'/entry/instrument/{slit_group_name}/{value_name}', 'f142', 'double')
            __add_data_stream(streams, motion_topic, f"{pv_name}.STAT",
                              f'/entry/instrument/{slit_group_name}/{value_name}_status', 'f142', 'int32')
            __add_data_stream(streams, motion_topic, f"{pv_name}.VELO",
                              f'/entry/instrument/{slit_group_name}/{value_name}_velocity', 'f142', 'double')

            __add_data_stream(streams, nicos_topic, f"nicos/{nicos_device_name}{nicos_value_name}/value",
                              f'/entry/instrument/{slit_group_name}/{value_name}_from_nicos_cache', 'ns10')

    _add_slit("Slit3", ["HZB-V20:MC-SLT-01:SltH-Center", "HZB-V20:MC-SLT-01:SltH-Gap", "HZB-V20:MC-SLT-01:SltV-Center",
                        "HZB-V20:MC-SLT-01:SltV-Gap"])

    links = {}

    converter = NexusToDictConverter()
    nexus_file = nexus.nxload(filepath)
    tree = converter.convert(nexus_file, streams, links)
    # The Kafka broker at V20 is v20-udder1, but due to the network setup at V20 we have to use the IP: 192.168.1.80
    # Use a timestamp in the output filename, but avoid characters "-" and ":"
    iso8601_str_seconds = datetime.now().isoformat().split('.')[0]
    timestamp = iso8601_str_seconds.replace(':', '_')
    timestamp = timestamp.replace('-', '_')
    start_time = 'STARTTIME'  # NICOS replaces STARTTIME
    stop_time = None
    file_name = 'FILENAME'  # NICOS replaces FILENAME
    write_command, stop_command = create_writer_commands(tree,
                                                         '/data/kafka-to-nexus/FILENAME',
                                                         broker='192.168.1.80:9092',
                                                         start_time=start_time,
                                                         stop_time=stop_time)
    object_to_json_file(write_command, 'V20_file_write_start.json')
    object_to_json_file(stop_command, 'V20_file_write_stop.json')


def __add_data_stream(streams, topic, source, path, module, type=None):
    options = {
        'topic': topic,
        'source': source,
        'writer_module': module
    }
    if type is not None:
        options['type'] = type

    if module == 'ev42':
        options['adc_pulse_debug'] = True
        options['nexus'] = '{"indices": {"index_every_mb": 1}}'
    elif module == 'f142':
        # Add cue entries each megabyte for log data
        options['nexus'] = '{"indices": {"index_every_mb": 1}}'

    streams[path] = options


def __add_sample_env_device(group_name, name, description=None):
    env_group = builder.add_nx_group(builder.get_root()['instrument'], group_name, 'NXenvironment')
    builder.add_dataset(env_group, 'name', name)
    if description is not None:
        builder.add_dataset(env_group, 'description', description)
    return env_group


def __add_attributes(node, attributes):
    for key in attributes:
        if isinstance(attributes[key], str):
            # Since python 3 we have to treat strings like this
            node.attrs.create(key, np.array(attributes[key]).astype('|S' + str(len(attributes[key]))))
        else:
            node.attrs.create(key, np.array(attributes[key]))


if __name__ == '__main__':
    output_filename = 'V20_example.nxs'
    input_filename = 'adc_test8_half_cover_w_waveforms.nxs'  # None
    nx_entry_name = 'entry'
    # compress_type=32001 for BLOSC, or don't specify compress_type and opts to get non-compressed datasets
    with NexusBuilder(output_filename, input_nexus_filename=input_filename, nx_entry_name=nx_entry_name,
                      idf_file=None, compress_type='gzip', compress_opts=1) as builder:
        instrument_group = builder.add_instrument('V20', 'instrument')
        builder.add_user('Person 1', 'ESS', number=1)
        builder.add_user('Person 2', 'STFC', number=2)
        __add_detector(builder)
        __add_choppers(builder)
        __add_monitors(builder)
        __add_motion_devices(builder)

        # Sample
        sample_group = builder.add_sample()
        builder.add_dataset(sample_group, 'description', '')
        builder.add_dataset(sample_group, 'name', '')
        builder.add_dataset(sample_group, 'chemical_formula', '')
        builder.add_dataset(sample_group, 'mass', 0, {'units': 'g'})
        record_z_position(builder, sample_group, 3.6)

        # Add a source at the position of the first chopper
        source = builder.add_source('V20_14hz_chopper_source', 'source', [0.0, 0.0, -25.3])
        builder.add_dataset(source, 'probe', 'neutron')

        # Add start_time dataset (required by Mantid)
        iso8601_str_seconds = datetime.now().isoformat().split('.')[0]
        builder.add_dataset(builder.get_root(), 'start_time', '8601TIME')  # NICOS replaces 8601TIME
        builder.add_dataset(builder.root, 'title', 'TITLE')  # NICOS replaces TITLE

        # Copy event data into detector
        __copy_existing_data()

        # Notes on geometry:

        # Geometry is altered slightly from reality such that analysis does not require handling the curved guides
        # and can treat the neutron paths as straight lines between source and sample, and sample and detector.

        # Since we will use timestamps from the first (furthest from detector) chopper as the pulse timestamps,
        # the "source" is placed at the position of the first chopper

        # kafkacat -b 192.168.1.80 -t V20_writerCommand -X message.max.bytes=20000000 V20_file_write_stop.json -P

    __create_file_writer_command(output_filename)
