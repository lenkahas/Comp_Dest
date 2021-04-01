import numpy, pandas
import data
from sklearn.preprocessing import LabelEncoder


from numba import njit, prange
from joblib import Parallel, delayed


def access_numba(flows, filter_flow=False, n_jobs=-1):

    origins = flows.origin.unique()
    destinations = flows.destination.unique()

    encoder = LabelEncoder().fit(numpy.hstack((origins, destinations)))

    flows["ocode"] = encoder.transform(flows.origin)
    flows["dcode"] = encoder.transform(flows.destination)

    all_masses = numpy.ones(encoder.classes_.shape[0]) * -1
    all_masses[flows.ocode] = flows.mass_origin
    all_masses[flows.dcode] = flows.mass_destination
    assert (all_masses >= 0).all()

    # accs = _access_numba(
    #    flows.ocode.values,
    #    flows.dcode.values,
    #    all_masses,
    #    flows.weight.values if filter_flow else numpy.ones_like(flows.weight.values),
    #    flows.distance.values,
    # )

    engine = Parallel()
    promise = delayed(_access_numba_i)

    fakeflows = numpy.ones_like(flows.weight.values)
    if n_jobs == 1:
        accs = _access_numba(
            flows.ocode.values,
            flows.dcode.values,
            all_masses,
            flows.weight.values if filter_flow else fakeflows,
            flows.distance.values,
        )
    else:
        accs = engine(
            promise(
                i,
                flows.ocode.values,
                flows.dcode.values,
                all_masses,
                flows.weight.values if filter_flow else fakeflows,
                flows.distance.values,
            )
            for i in range(flows.shape[0])
        )

    return flows.assign(accessibility=accs)


@njit(fastmath=True)
def _access_numba(origins, destinations, all_masses, weights, distances):
    n_flows = weights.shape[0]

    accessibilities = numpy.zeros(weights.shape, dtype=weights.dtype)
    for flowix in range(n_flows):
        accessibilities[flowix] = _access_numba_i(
            flowix, origins, destinations, all_masses, weights, distances
        )
    return accessibilities


@njit(fastmath=True)
def _access_numba_i(flowix, origins, destinations, all_masses, weights, distances):
    oix = origins[flowix]
    dix = destinations[flowix]
    if oix == dix:
        return 0
    from_origin = origins == oix
    to_destination = destinations == dix
    from_destination = origins == dix
    competitors = destinations[from_origin & (~to_destination)]
    acc_d = 0
    for competitor in competitors:
        if competitor == oix:
            continue
        if competitor == dix:
            continue
        to_competitor = destinations == competitor
        if not (from_destination & to_competitor).any():
            continue
        if not (from_origin & to_competitor).any():
            continue
        d_dest_to_comp = distances[from_destination & to_competitor]
        w_dest_to_comp = weights[from_origin & to_competitor] > 0
        wdist = (all_masses[competitor] * d_dest_to_comp * w_dest_to_comp)[0]
        acc_d += wdist
    return acc_d


if __name__ == "__main__":
    access_numba(data.toy())
