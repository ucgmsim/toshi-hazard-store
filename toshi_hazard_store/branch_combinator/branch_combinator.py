# rom toshi_hazard_store.branch_combinator.SLT_test1 import *
import itertools
import json
import math
from collections import namedtuple

DTOL = 1.0e-6


def get_branches():
    assert 0


def get_weighted_branches(grouped_ltbs):

    # TODO: only handles one combined job and one permutation set
    permute = grouped_ltbs  # tree_permutations[0][0]['permute']

    # check that each permute group weights sum to 1.0
    for key, group in permute.items():
        group_weight = 0
        for member in group:
            group_weight += member.weight
        if (group_weight < 1.0 - DTOL) | (group_weight > 1.0 + DTOL):
            print(len(group), 'items', group_weight)
            print(group)
            raise Exception(f'group {key} weight does not sum to 1.0')

    # do the thing
    id_groups = []
    for key, group in permute.items():
        id_group = []
        for member in group:
            id_group.append({'id': member.hazard_solution_id, 'weight': member.weight})
        id_groups.append(id_group)

    print(id_groups)

    branches = itertools.product(*id_groups)
    source_branches = []
    for i, branch in enumerate(branches):
        name = str(i)
        ids = [leaf['id'] for leaf in branch]
        weights = [leaf['weight'] for leaf in branch]
        weight = math.prod(weights)
        branch_dict = dict(name=name, ids=ids, weight=weight)
        source_branches.append(branch_dict)

    # double check that the weights are done correctly

    weight = 0
    for branch in source_branches:
        weight += branch['weight']
    if not ((weight > 1.0 - DTOL) & (weight < 1.0 + DTOL)):
        print(weight)
        raise Exception('weights do not sum to 1')

    return source_branches


Member = namedtuple("Member", "group tag weight inv_id bg_id hazard_solution_id")


def weight_and_ids(data):
    def get_tag(args):
        for arg in args:
            if arg['k'] == "logic_tree_permutations":
                return json.loads(arg['v'].replace("'", '"'))[0]['permute']  # ['members'][0]
        assert 0

    nodes = data['data']['node1']['children']['edges']
    for obj in nodes:
        tag = get_tag(obj['node']['child']['arguments'])
        hazard_solution_id = obj['node']['child']['hazard_solution']['id']
        yield Member(**tag[0]['members'][0], group=None, hazard_solution_id=hazard_solution_id)


def all_members_dict(ltbs):
    """LTBS from ther toshi GT - NB some may be failed jobs..."""
    res = {}

    def members():
        for grp in ltbs[0][0]['permute']:
            # print(grp['group'])
            for m in grp['members']:
                yield Member(**m, group=grp['group'], hazard_solution_id=None)

    for m in members():
        res[f'{m.inv_id}{m.bg_id}'] = m
    return res


def merge_ltbs(logic_tree_permutations, gtdata, omit):
    members = all_members_dict(logic_tree_permutations)
    # weights are the actual Hazard weight @ 1.0
    for toshi_ltb in weight_and_ids(gtdata):
        if toshi_ltb.hazard_solution_id in omit:
            print(f'skipping {toshi_ltb}')
            continue
        d = toshi_ltb._asdict()
        d['weight'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].weight
        d['group'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].group
        yield Member(**d)


def merge_ltbs_fromLT(logic_tree_permutations, gtdata, omit):
    members = all_members_dict(logic_tree_permutations)
    # weights are the actual Hazard weight @ 1.0
    for toshi_ltb in weight_and_ids(gtdata):
        if toshi_ltb.hazard_solution_id in omit:
            print(f'skipping {toshi_ltb}')
            continue
        if members.get(f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'):
            d = toshi_ltb._asdict()
            d['weight'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].weight
            d['group'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].group
            yield Member(**d)


def grouped_ltbs(merged_ltbs):
    grouped = {}
    for ltb in merged_ltbs:
        if ltb.group not in grouped:
            grouped[ltb.group] = []
        grouped[ltb.group].append(ltb)
    return grouped
