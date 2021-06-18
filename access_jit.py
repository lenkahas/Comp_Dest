import numpy, pandas
from sklearn.preprocessing import LabelEncoder

try:
    from numba import njit
except ImportError:

    def njit(fastmath):
        def decorator(func):
            return func

        return decorator


def access(flows, filter_flow=False, n_jobs=-1):

    origins = flows.origin.unique()
    destinations = flows.destination.unique()

    encoder = LabelEncoder().fit(numpy.hstack((origins, destinations)))

    flows["ocode"] = encoder.transform(flows.origin)
    flows["dcode"] = encoder.transform(flows.destination)

    all_masses = numpy.ones(encoder.classes_.shape[0]) * -1
    all_masses[flows.ocode] = flows.mass_origin
    all_masses[flows.dcode] = flows.mass_destination
    assert (all_masses >= 0).all()

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
        from joblib import Parallel, delayed

        engine = Parallel()
        promise = delayed(_access_numba_i)
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
    import data
    a1 = access(data.toy())

    numpy.testing.assert_array_equal(
        a1.accessibility,
        [
            0,  # A_aa is always zero
            30 * 20,  # A_ab is mass of c times distance from b to c
            25 * 20,  # A_ac is mass of b times distance from c to b
            30 * 10,  # A_ba is mass of c times distance from a to c
            0,  # A_bb is always zero
            60 * 10,  # A_bc is mass of a times the distance from c to a
            25 * 2,  # A_ca is mass of b times distance from a to b
            60 * 2,  # A_cb is mass of a times distance from b to a
            0,  # A_cc is always zero
        ],
    )
    print("passed no filter.")
    a2 = access_slow(data.toy(), filter_flow=True)

    numpy.testing.assert_array_equal(
        a2.accessibility,
        [
            0,  # A_aa is always zero
            30 * 20,  # A_ab is mass of c times distance from b to c if flow a -> c
            25 * 20 * 0,  # A_ac is mass of b times distance from c to b if flow a -> b
            30 * 10,  # A_ba is mass of c times distance from a to c if flow b -> c
            0,  # A_bb is always zero
            60 * 10,  # A_bc is mass of a times the distance from c to a if flow b -> a
            25 * 2,  # A_ca is mass of b times distance from c to b if flow c -> b
            60 * 10 * 0,  # A_cb has no routes from c to a if flow c -> a
            0,  # A_cc is always zero
        ],
    )
    print("passed with filter.")

