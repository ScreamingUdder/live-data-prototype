import flatbuffers
import numpy as np
from flat_buf.FlatbufEventData import *


class FlatBuffersUtils(object):
    @staticmethod
    def encode_event_data(data):
        # This seems inefficient because we have to construct the two vectors separately
        # Is the way to avoid this?
        builder = flatbuffers.Builder(0)

        # Construct TOF vector
        FlatbufEventDataStartTofVector(builder, len(data['tof']))

        # Buffers are created backwards
        for i in reversed(data['tof']):
            builder.PrependUint64(i)
        tof = builder.EndVector(len(data['tof']))

        # Construct Det_ID vector
        FlatbufEventDataStartDetIdVector(builder, len(data['detector_id']))

        for i in reversed(data['detector_id']):
            builder.PrependUint32(i)
        det = builder.EndVector(len(data['detector_id']))

        # Build the buffer
        FlatbufEventDataStart(builder)
        FlatbufEventDataAddTof(builder, tof)
        FlatbufEventDataAddDetId(builder, det)
        FlatbufEventDataAddCount(builder, len(data))
        data = FlatbufEventDataEnd(builder)
        builder.Finish(data)

        return builder.Output()

    @staticmethod
    def decode_event_data(buf):
        raw = FlatbufEventData.GetRootAsFlatbufEventData(buf, 0)

        # Preallocate the space for the array
        data = np.zeros((raw.Count(),), dtype=[('detector_id', '<i4'), ('tof', '<f4')])

        for i in range(raw.Count()):
            data[i] = (raw.DetId(i), raw.Tof(i))

        return data
