"""
See https://stackoverflow.com/questions/50861237/is-there-an-alternative-to-difflib-get-close-matches-that-returns-indexes-l

TODO:
- allow abbreviations to be matched as the same word for street name post type
"""

from difflib import SequenceMatcher
from heapq import nlargest as _nlargest


def get_close_matches_indexes(word, possibilities, n=3, cutoff=0.6):
    """
    Use SequenceMatcher to return a list of the indexes of the best "good enough" matches.

    Parameters
    ----------
    word [str] : Sequence for which close matches are desired.
    possibilities [list] : List of sequences match word against.

    Optional Parameters
    -------------------
    n [default = 3, int] : Maximum number of close matches to return.  n must be > 0.
    cutoff [default = 0.6, float] : Score in range of [0, 1] to consider.

    Returns
    -------
    (score, x) [tuple] : score and index of element(s) in possibilities with the highest score(s)
    """

    if not n > 0:
        raise ValueError("n must be > 0: %r" % (n,))
    if not 0.0 <= cutoff <= 1.0:
        raise ValueError("cutoff must be in [0.0, 1.0]: %r" % (cutoff,))
    result = []
    s = SequenceMatcher(autojunk=False)
    s.set_seq2(word)
    for idx, x in enumerate(possibilities):
        s.set_seq1(x)
        if (
            s.real_quick_ratio() >= cutoff
            and s.quick_ratio() >= cutoff
            and s.ratio() >= cutoff
        ):
            result.append((s.ratio(), idx))

    # Move the best scorers to head of list
    result = _nlargest(n, result)
    
    # Strip scores for the best n matches
    return [(score, x) for score, x in result]
