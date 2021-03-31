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
        600,  # A_ab is mass of c times distance from b to c
        300,  # A_ba is mass of c times distance from a to c
        350,  # A_aa is mass of b times distance from a to b plus mass of c times distance from a to c
        0,  # A_cb has no routes from c to a
        0,  # A_bc has no routes from c to a
        500,  # A_ac is mass of b times distance from c to b
    ],
)
print("passed no filter.")
a2 = access_slow(toy, filter_flow=True)

numpy.testing.assert_array_equal(
    a2,
    [
        600,  # A_ab, mass of c times distance from b to c
        300,  # A_ba, mass of c times distance from a to c
        300,  # A_aa, mass of c times distance from a to c. Ignore mass of b times distance from a to b because b has no flow from a.
        0,  # A_cb, no routes from c to a
        0,  # A_bc, no routes from c to a
        0,  # A_ac, Ignore mass of b times distance from c to b because b is has no flow from a.
    ],
)
print("passed with filter.")
