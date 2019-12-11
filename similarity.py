import string
from strsimpy.ngram import NGram
from strsimpy.jaccard import Jaccard

from typing import List, Union, Dict, Tuple


### STR PREPROCESSORS ###


def no_transform(x):
    return x


def rm_all_but_alphanum(s: str) -> str:
    """
    remove all possible whitespace leaving only alphanumeric characters
    """
    remove_chars = string.punctuation + string.whitespace

    s_translated = s.translate(
        str.maketrans('', '', remove_chars))
    return s_translated


def repl_nums(s: str, char_replace: str = '*') -> str:
    """
    replace nums with char_replace
    """
    s_translated = s.translate(
        str.maketrans(string.digits, char_replace * len(string.digits)))
    return s_translated


def std_preprocess(s: str) -> str:
    """
    leaves only alphabetical chars the same, changes numbers to 1 symbol, removes whitespace
    """
    return repl_nums(rm_all_but_alphanum(s.lower()), '*')


def lower(s: str) -> str:
    return s.lower()


### USED FOR STR COMPARISONS ###


def shortest_str(s0: str, s1: str) -> str:
    if len(s0) < len(s1):
        return s0
    return s1


def shortest_word(s: str) -> int:
    return min([len(s) for s in s.split(' ')])


### COMPARISON FUNCTIONS ###


"""
use like:
`cutoff(comp_ngram('science and physics', 'science', std_preprocess), 0.5)`

or:
`comp_substr('science and physics', 'science', lower)`

or to stack preprocessors:
`cutoff(comp_ngram('science and physics', 'science', lambda s: lower(std_preprocess(s))), 0.5)`
"""

S_TYPE = 0
COL = 1
DIST = 2
ADJ_DIST = 3

MIN_DIST = 2
AVG_DIST = 3

def comp_substr(s0: str, s1: str, pre_process=no_transform) -> bool:
    """
    test two strings for whether they are identical by testing whether one string is in the other

    definitely use for column names
    """
    proc_s0 = pre_process(s0)
    proc_s1 = pre_process(s1)
    return (proc_s0 in proc_s1) or (proc_s1 in proc_s0)


def comp_substr_target(s0: str, s1: str, pre_process=no_transform) -> bool:
    """
    test two strings for whether they are identical by testing whether s0 is in s1

    definitely use for column names
    """
    proc_s0 = pre_process(s0)
    proc_s1 = pre_process(s1)
    return (proc_s0 in proc_s1)


def comp_ngram(s0: str, s1: str, pre_process=no_transform) -> float:
    proc_s1 = pre_process(s0)
    proc_s2 = pre_process(s1)

    ngram = NGram(len(shortest_str(s0, s1))//2)
    dist = ngram.distance(proc_s1, proc_s2)
    # print('strs:', proc_s1, '|', proc_s2)
    # print('distance:', dist)

    return dist


def comp_jac(s0: str, s1: str, pre_process=no_transform) -> float:
    """
    jaccard comparison based on k-shingle half of the longest string. good for high similarity

    can be used for column values
    """
    proc_s1 = pre_process(s0)
    proc_s2 = pre_process(s1)

    # jac = Jaccard(len(shortest_str(s0, s1))//2)
    jac = Jaccard(3)
    dist = jac.distance(proc_s1, proc_s2)
    # print('strs:', proc_s1, '|', proc_s2)
    # print('distance:', dist)

    return dist


def cutoff(distance, cutoff):
    return distance < cutoff


### MATCHING FUNCTIONS ###


def match_substr(s: str, dct: Dict[str, List[str]],
                 preprocess=no_transform) -> Union[Tuple[str, str, float],
                                                   None]:
    """
    match s to a value in dct and return the matched (key, value) pair accordingly, along with the distance value of value from s. 
    I.e. match a column name with unknown semantic type to a column name with known semantic type

    match works as follows:
    there are 3 stages. 
    1. iterate through the values in dct and add those values that are either, in s, or which s is in, to a list that will move onto stage 2.
    2. take the jaccard similarity distance of s to all the values in stage 2
    3. return a tuple pair of the key and value with min distance

    note: use this when it is likely there will be an exact match (column names). used this to locate ds column names based on ta column names

    returns: Tuple[key, matched value, distance] or None if no substrs matched first

    example:
    >>match('Core Subject 9,10', {'Subject': ['core subject 123 12', 'Core Subject 9,10,11']}, rm_all_but_alphanum)
    ('Subject', 'Core Subject 9,10,11', 0.5)
    """
    stage_2 = []
    for s_type, lst_targets in dct.items():
        for target in lst_targets:
            if s == target:  # identical match to skip further steps
                return (s_type, target, 0.0)
            elif comp_substr(s, target, preprocess):
                stage_2.append((s_type, target, None))

    if len(stage_2) == 0:
        return None
    # print('stage_2:', stage_2)

    final_stage = []
    for s_type, target, _ in stage_2:
        dist = comp_jac(s, target, preprocess)
        res = (s_type, target, dist)
        if dist == 0.0:
            return res
        else:
            final_stage.append(res)
    # print('final_stage:', final_stage)

    return min(final_stage, key=lambda x: x[DIST])


def match_jacc_min(s: str, dct: Dict[str, List[str]],
               preprocess=no_transform) -> Union[Tuple[str, str, float], None]:
    """
    match s to a value in dct and return the matched key, value pair accordingly with distance of value from s. 
    I.e. match a column value with unknown semantic type to a column value with known semantic type

    match_jac works as follows:
    there are 2 stages (no substr matching unlike match_substr). 
    1. take the jaccard similarity distance of s to all the values in stage 2
    2. return a tuple pair of the key and value with min distance

    note: use this for when there may or may not be an exact match (since this relies only on jaccard). 
    this method can give more false positives with very similar items.
    so it should be good for column values but not column names

    example:
    >>match_jacc_min('Core Subject 9,10', {'Subject': ['core subject 123 12', 'Core Subject 9,10,11']}, rm_all_but_alphanum)
    ('Subject', 'Core Subject 9,10,11', 0.5)
    """
    stage_2 = []
    for s_type, lst_targets in dct.items():
        for target in lst_targets:
            # put in all, minimizes code rewriting from match
            stage_2.append((s_type, target, None))

    if len(stage_2) == 0:
        return None
    # print('stage_2:', stage_2)

    final_stage = []
    for s_type, target, _ in stage_2:
        dist = comp_jac(s, target, preprocess)
        res = (s_type, target, dist)
        if dist == 0.0:
            return res
        else:
            final_stage.append(res)
    # print('final_stage:', final_stage)

    return min(final_stage, key=lambda x: x[DIST])


def match_jacc_avg(s: str, dct: Dict[str, List[str]],
               preprocess=no_transform) -> Union[Tuple[str, str, float], None]:
    """
    match s to a value in dct and return the matched key, value pair accordingly with distance of value from s. 
    I.e. match a column value with unknown semantic type to a column value with known semantic type

    match_jac works as follows:
    there are 2 stages (no substr matching unlike match_substr). 
    1. take the jaccard similarity distance of s to all the values in stage 2
    2. return a tuple pair of the key and value with min avg distance

    note: use this for when there may or may not be an exact match (since this relies only on jaccard). 
    this method can give more false positives with very similar items.
    so it should be good for column values but not column names

    example:
    >>match_jacc_min('Core Subject 9,10', {'Subject': ['core subject 123 12', 'Core Subject 9,10,11']}, rm_all_but_alphanum)
    ('Subject', 'Core Subject 9,10,11', 0.5)
    """
    sums_s_type = {}  # {s_type: sum_distance}
    for s_type, lst_targets in dct.items():
        for target in lst_targets:
            dist = comp_jac(s, target, preprocess)
            res = (s_type, target, dist)
            if dist == 0.0:
                return res

            S_TYPE = 0
            MIN_TARGET = 1
            MIN_DIST = 2
            SUM_DIST = 3  # to avg distance
            COUNT_DIST = 4  # to avg distance
            # accumulate
            if not s_type in sums_s_type:
                sums_s_type[s_type] = [s_type, None, None, 0, 0]
            
            min_dist = sums_s_type[s_type][MIN_DIST]
            if min_dist is None or dist < min_dist:
                sums_s_type[s_type][MIN_TARGET] = target
                sums_s_type[s_type][MIN_DIST] = dist
            
            sums_s_type[s_type][SUM_DIST] += dist
            sums_s_type[s_type][COUNT_DIST] += 1

    if len(sums_s_type) == 0:
        return None

    min_res = min(list(sums_s_type.items()), key=lambda x: x[1][SUM_DIST])[1]
    min_final = (min_res[S_TYPE], min_res[MIN_TARGET], min_res[MIN_DIST], (min_res[SUM_DIST] / min_res[COUNT_DIST]))
    # print('min_final:', min_final)

    return min_final


### MAIN MATCHING TECHNIQUE ###


def match_preprocess(s: str, dct: Dict[str, List[str]],
                     matchfn = match_jacc_min, weights: List[float] = [0.90, 0.95, 0.97, 1.00]) -> Union[Tuple[str, str, float, float],
                                                                                        None]:
    """
    best matching function.

    run matchfn (i.e. match_jacc_min, match_substr) through multiple preprocessing steps of increasing generality and return the result with min distance. 
    i.e. if a distance of 0 is found at the no_transform step then there is an exact match, if not, continue and see if there is an exact match without whitespace, etc.
    if there are no exact matches return the one with smallest distance.

    can assign weights to stages so that the early, less preprocessing stages are likelier to be selected.
    
    returns: Tuple[key, matched val, distance, adjusted distance] or None if no matches found

    example:
    >>match_preprocess('Core Subject 9,10', {'Subject': ['core subject 123 12', 'Core Subject 9,10,11']}, match_jacc_min)
    ('Subject', 'Core Subject 9,10,11', 0.2222222222222222, 0.21111111111111108)
    """
    # operations are repeated below
    # if the state of strings were to be saved after processing, then repeated operations can be avoided
    # however, this requires saving extra strings and extra code (not a priority)
    fns = [
        no_transform,
        lambda x: rm_all_but_alphanum(x),
        lambda x: rm_all_but_alphanum(lower(x)),
        lambda x: repl_nums(rm_all_but_alphanum(lower(x)))
    ]
    stages = []
    for fn, weight in zip(fns, weights):
        res = matchfn(s, dct, fn)
        # print('res:', res)
        if res is not None:
            res = list(res)  # convert to list for mutability

            dist = res[DIST]
            adj_dist = dist * weight  # adjust distance by weight
            res.append(adj_dist)

            res = tuple(res)  # convert back to tuple

            if res[DIST] == 0.0:
                return res
            else:
                stages.append(res)
    # print('stages:', stages)
    if len(stages) == 0:
        # print('no matches')
        return None
    return min(stages, key=lambda x: x[ADJ_DIST])
