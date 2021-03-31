#!/usr/bin/env python
# coding: utf-8

# In[1]:

import data, numpy

toy = data.toy()

print(toy)


def access_slow(flows, stop=True, filter_flow=False):
    accessibilities = []
    for row, flowvec in flows.iterrows():
        oid, did = flowvec.origin, flowvec.destination
        print(
            f"-----------------------------\nComputing accessibility of {did} to {oid}"
        )
        alternatives = flows.query("origin == @oid")
        print(f"\t{oid} can get to {alternatives.destination.tolist()}")
        accessibility = 0
        if oid == did:
            print(
                f"\t\torigin {oid} is exactly destination {did}, so accessibility is zero"
            )
            accessibilities.append(accessibility)
            continue
        for _, alternative in alternatives.iterrows():
            if stop:
                input()
            if alternative.destination == did:
                print(
                    f"\t\tSkipping {alternative.destination} because it is target destination {did}"
                )
                continue
            if alternative.destination == oid:
                print(
                    f"\t\tSkipping {alternative.destination} because it is target origin {oid}"
                )
                continue
            if filter_flow and (not (alternative.weight > 0)):
                print(
                    f"\t\tSkipping {alternative.destination} because it has negative or zero flow from {oid}"
                )
                continue
            this_alternative_from_destination = flows.query(
                "origin == @did & destination == @alternative.destination"
            )
            if this_alternative_from_destination.empty:
                print(
                    f"\t\tSkipping {alternative.destination} because there are no routes from {did} to {alternative.destination}"
                )
                continue
            ak = this_alternative_from_destination.eval(
                "mass_destination * distance"
            ).item()
            print(
                f"\t\t\t A_{oid},{did} gets {this_alternative_from_destination.mass_destination.item()}*{this_alternative_from_destination.distance.item()} from {alternative.destination}"
            )
            accessibility += ak
        accessibilities.append(accessibility)
    return numpy.asarray(accessibilities)


a1 = access_slow(toy)
print(toy)

numpy.testing.assert_array_equal(
    a1,
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
a2 = access_slow(toy, filter_flow=True)

numpy.testing.assert_array_equal(
    a2,
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


t1 = (
    toy.eval("wdist = distance*mass_destination")
    .query("origin != destination")
    .groupby("origin")
    .wdist.sum()
    .to_frame("wdist")
    .merge(toy, left_index=True, right_on="destination", how="right")
    .eval(
        "acc_groupby_nofilter = (wdist - mass_origin*distance)*(origin != destination)"
    )
)
print(t1.assign(acc_nofilter=a1, acc_filter=a2))
numpy.testing.assert_array_equal(t1.acc_groupby_nofilter, a1)
