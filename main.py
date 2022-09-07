"""

    """

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq


class GDUrl :
    src = 'https://github.com/imahdimir/d-CodalTicker-2-ISIC'
    cur = 'https://github.com/imahdimir/u-d-CodalTicker-2-ISIC'
    trg = 'https://github.com/imahdimir/d-FirmTicker-2-ISIC'
    c2f = 'https://github.com/imahdimir/d-CodalTicker-2-FirmTicker'

gdu = GDUrl()

class ColName :
    ctic = 'CodalTicker'
    ftic = 'FirmTicker'
    isic = 'ISIC'
    obsd = 'ObsDate'

c = ColName()

def main() :
    pass

    ##
    # Get source data
    gd_src = GithubData(gdu.src)
    gd_src.overwriting_clone()
    ##
    ds = gd_src.read_data()
    ##

    gd_c2f = GithubData(gdu.c2f)
    gd_c2f.overwriting_clone()
    ##
    dc = gd_c2f.read_data()
    ##
    dc.set_index(c.ctic , inplace = True)

    ##
    ds[c.ftic] = ds[c.ctic].map(dc[c.ftic])
    ##
    msk = ~ dc[c.ftic].isin(ds[c.ftic])
    d1 = dc[msk]

    ##
    ds.dropna(inplace = True)
    ##

    gd_trg = GithubData(gdu.trg)
    gd_trg.overwriting_clone()
    ##
    dap = gd_trg.data_fp
    da = gd_trg.read_data()
    ##
    da = pd.concat([da , ds])
    ##
    da.drop_duplicates(inplace = True)
    ##
    da = da[[c.ftic , c.ctic , c.isic , c.obsd]]
    ##
    sprq(da , dap)
    ##
    msg = 'updated by: '
    msg += gdu.cur
    ##
    gd_trg.commit_and_push(msg)

    ##


    gd_trg.rmdir()
    gd_c2f.rmdir()
    gd_src.rmdir()


    ##

    ##

##
if __name__ == "__main__" :
    main()

##
# noinspection PyUnreachableCode
if False :
    pass

    ##
    da.drop(columns = c.obsd , inplace = True)

    ##
    from mirutil.df_utils import save_df_as_a_nice_xl as sxl


    ##
    sxl(da , 'Firm-ISIC.xlsx')


    ##
