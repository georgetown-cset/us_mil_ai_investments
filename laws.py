# LAWS project
import pandas as pd
import re
from google.cloud import storage, bigquery
from helpers.gcs_storage import list_blobs, delete_blob, download_blob, upload_blob
import os
#  functions
# calculate the number of AI and Robotics keywords.
def count_list(list, desc):
    count = 0
    for v in list:
        v = v.lower()
        desc = desc.lower()
        # somehow ignore case does not work.
        if re.search(v, desc, re.IGNORECASE):
            count += 1
    return count

def download_data():
    #download data on Pentagon projects
    download_blob('cset_financial_data', 'LAWS/MASTER.csv', 'Data/MASTER.csv')
    download_blob('cset_financial_data', 'LAWS/AI_keywords.csv', 'Data/AI_keywords.csv')
    # read data into pandas
    data = pd.read_csv('Data/Master.csv')
    keywords = pd.read_csv('Data/AI_keywords.csv')
    # List of variables and keywords
    cost_list = keywords.loc[keywords['cost_list'].notna(),'cost_list'].to_list()
    autonomy_list = keywords.loc[keywords['autonomy_list'].notna(),'autonomy_list'].to_list()
    AI_list = keywords.loc[keywords['AI_list'].notna(),'AI_list'].to_list()
    robotics_list = keywords.loc[keywords['robotics_list'].notna(),'robotics_list'].to_list()
    other_list = keywords.loc[keywords['other_list'].notna(),'other_list'].to_list()
    hum_mach_list = keywords.loc[keywords['hum_mach_list'].notna(),'hum_mach_list'].to_list()
    data['Full_Component Description'] = data[['Component Description','Component FY 2018 Plans', 'Component FY 2019 Plans',
                                               'Component FY 2020 Plans' ]].fillna('').sum(axis=1)
    # merge lists in the combinations Rita is interested in
    autonomy_other_list = autonomy_list + other_list
    ai_rob_list = AI_list + robotics_list
    autonomy_ai_list = autonomy_list + AI_list
    autonomy_other_ai_list = autonomy_list + AI_list + other_list
    autonomy_rob_ai_list = autonomy_list + AI_list + robotics_list
    autonomy_rob_ai_other_list = autonomy_list + AI_list + robotics_list + other_list
    # names the lists for file naming
    # covert data to numeric
    data[cost_list] = data[cost_list].apply(pd.to_numeric, errors='coerce').fillna(0)
    # Calculate interaction between ai_rob and hum_mach
    return data, cost_list, autonomy_list,AI_list, robotics_list, other_list, hum_mach_list, autonomy_other_list, ai_rob_list,\
           autonomy_other_ai_list, autonomy_rob_ai_other_list, autonomy_rob_ai_list,autonomy_ai_list





# We calculated the presentce and the number of keywords in project description and also tag project expenditures
# according to the most generous AI definition (max AI). Max AI: all PEs with AI keywords + all projects from non-AI PEs
# + all components from non-AI PEs and non-AI Projects. This avoids double counting of AI

def identify_ai_projects(data):
    lists = [autonomy_list, AI_list, autonomy_other_list, ai_rob_list, autonomy_ai_list , autonomy_other_ai_list ,
              autonomy_rob_ai_list, autonomy_rob_ai_other_list, hum_mach_list ]
    list_names = ['autonomy_list', 'AI_list' , 'autonomy_other_list', 'ai_rob_list', 'autonomy_ai_list', 'autonomy_other_ai_list' ,
              'autonomy_rob_ai_list', 'autonomy_rob_ai_other_list', 'hum_mach_list']
    level = ['PE', 'Project', 'Component']
    for i in range(0,len(lists)):
        for l in level:
            col_name_binary = l + '_' + list_names[i][:-5]
            col_name_intensity = l + '_intensity_' + list_names[i][:-5]
            if l == 'Component':
                data[col_name_binary] = data['Full_Component Description'].apply(lambda x: 0 if count_list(lists[i], str(x)) == 0 else 1)
                data[col_name_intensity] = data['Full_Component Description'].apply(lambda x: count_list(lists[i], str(x)))
            else:
                data[col_name_binary] = data[l + ' Description'].apply(lambda x: 0 if count_list(lists[i], str(x)) == 0 else 1)
                data[col_name_intensity] = data[l + ' Description'].apply(lambda x: count_list(lists[i], str(x)))
        max_name = 'max_' + list_names[i][:-5]
        data.loc[data['PE_' + list_names[i][:-5]] == 1, max_name] = 1
        data.loc[(data['PE_' + list_names[i][:-5]] == 0) & (data['Project_' + list_names[i][:-5]] == 1),  max_name] = 1
        data.loc[(data['PE_' + list_names[i][:-5]] == 0) & (data['Project_' + list_names[i][:-5]] == 0) &
                 (data['Component_' + list_names[i][:-5]] == 1), max_name] = 1
    #Identify autonomy and AI projects together:
    level_m = ['PE', 'Project', 'Component', 'max']
    for l in level_m:
        data.loc[(data[l + '_autonomy'] == 1) & (data[l + '_AI'] == 1), l + '_autonomy_AND_AI'] = 1
        data.loc[data[l + '_autonomy_AND_AI'].isna(), l + '_autonomy_AND_AI'] = 0
    ###### Count results
        print(f"{l} counts: Autonomy {data[l + '_autonomy'].sum()}, AI {data[l + '_AI'].sum()}, autonomy_OR_AI "
              f"{data[l + '_autonomy_ai'].sum()}, autonomy_AND_AI {data[l + '_autonomy_AND_AI'].sum()}")
    return data


def calculate_costs(data):
    print(data['max_AI'].sum(), " Total of AI max expenditures")
    print(data['PE_AI'].sum(), " Total of AI PE expenditures")
    print(data['Component_AI'].sum(), " Total of AI Component expenditures")
    print(data['Project_AI'].sum(), " Total of AI Project expenditures")
    # Calculate total costs for PE, Project and Component for all years
    data['PE Cost Total_calc'] = data.loc[:,[c for c in cost_list if ('PE' in c) & ~('Total' in c)]].sum(axis=1)
    data['Project Cost Total_calc']= data.loc[:,[c for c in cost_list if ('Project' in c) & ~('Total' in c)]].sum(axis=1)
    data['Component Cost Total_calc']= data.loc[:,[c for c in cost_list if ('Component' in c) & ~('Total' in c)]].sum(axis=1)
    level = ['Project_', 'Component_', 'PE_']
    for l in level:
        data.loc[(data[l + 'ai_rob'] == 1) & (data[l + 'hum_mach'] == 1), l + 'rob_hum_mach'] = 'both'
        data.loc[(data[l + 'ai_rob'] == 0) & (data[l + 'hum_mach'] == 1), l + 'rob_hum_mach'] = 'hum_mach_only'
        data.loc[(data[l + 'ai_rob'] == 1) & (data[l + 'hum_mach'] == 0), l + 'rob_hum_mach'] = 'ai_rob_only'
    # Save updated master file to be sent to Rita
    data.to_csv('Data/Updated_master.csv')
    PE_all = [c for c in cost_list if ('PE' in c) & ~('Total' in c)] + ['PE Cost Total_calc']
    PR_all = [c for c in cost_list if ('Project' in c) & ~('Total' in c)] + ['Project Cost Total_calc']
    CO_all = [c for c in cost_list if ('Component' in c) & ~('Total' in c)] + ['Component Cost Total_calc']
# Arrays of costs for different project levels.
    return data, PE_all, PR_all , CO_all


# Calculate top most expenive AI projects for difference levels, and categories.
# levels: 'PE', 'Project', 'Component'
def top20(level, science, cat, cat_value, intensity = 0):
    # name of the tab in excel file
    outname = 'top20_' + level[0:2] + '_' + science + '_' + cat_value
    # restict tab name to the maximum allowed 31 characters. Excel does not allow for more.
    outname = outname[:31]
    # science is the type of AI keywords: AI, robotics, etc.
    level_var = level + '_' + science
    #tot_cost = 'Total ' + level + '  Cost Ilya'
    if level == 'PE':
        # default is to calcualte the presence of AI keyword and sort the projects according to the costs and if the
        # keywords are present
        sort_var = 'PE Cost Total_calc'
        temp = level + '_' + science
        # If intensity is 1, we calculate the number of keywords, not just indicate their presence
        # We sort projects first by number of keywords and then by the costs
        if (intensity == 1) & (science != 'rob_hum_mach'):
            temp = 'PE_intensity_' + science
            sort_var = [temp, 'PE Cost Total_calc']
            # return top 20 most expendive and relevant projects
        out = data[['PE Name', 'PE Description', 'PE Number', temp, 'Service', 'Research Category'] + PE_all]\
        .loc[(data[level_var] == 1) & \
        (data[cat]== cat_value)].drop_duplicates().\
        sort_values(by= sort_var, ascending = False)[:20]
    elif level == 'Project':
        sort_var = 'Project Cost Total_calc'
        temp = level + '_' + science
        # There is not intensity calcluation for the humane_machine_internaction
        if (intensity == 1) & (science != 'rob_hum_mach'):
            temp = 'Project_intensity_' + science
            sort_var = [temp, 'Project Cost Total_calc']
        out = data[['Unique Project Number','Project Name', 'Project Description', temp, 'Service', 'Research Category']
                   + PR_all].loc[(data[level_var] == 1) & (data[cat]== cat_value)].drop_duplicates().\
                    sort_values(by= sort_var, ascending = False)[:20]
    elif level == 'Component':
        sort_var = 'Component Cost Total_calc'
        temp = level + '_' + science
        if (intensity == 1) & (science != 'rob_hum_mach'):
            temp = 'Component_intensity_' + science
            sort_var = [temp, 'Component Cost Total_calc']
        out = data[['Component Description', 'Unique Component Number', 'Component Title',temp, 'Service','Research Category']\
                   + CO_all].loc[(data[level_var] == 1) & (data[cat]== cat_value)].drop_duplicates().\
            sort_values(by= sort_var, ascending = False)[:20]
    # export table and tab_name
    return out, outname


def sum_tab(level, science, cat, sum = True):
    # aggregation by project level and category (NAVY, Basic Science, etc)
    level_cat_sc =  [cat, level + '_' + science]
    # table name in the excel file
    if sum:
        outname = 'Sum_' + level + '_' + science + '_' + cat
    else:
        outname = 'cnt_' + level + '_' + science + '_' + cat
# restict tab name to the maximum allowed 31 characters. Excel does not allow for more.
    outname = outname[:31]
    # find the right project level
    if level == 'Project':
        # aggregate projects according to category [cat]
        # Return costs specified in PR_all
        agg_data = data[PR_all + level_cat_sc + ['Unique Project Number']].drop_duplicates()
        if sum:
            out = agg_data[PR_all + level_cat_sc].groupby(level_cat_sc).sum()
        else:
            out = agg_data[PR_all + level_cat_sc].groupby(level_cat_sc).count()
    if level == 'PE':
        agg_data = data[PE_all + level_cat_sc + ['PE Number']].drop_duplicates()
        if sum:
            out = agg_data[PE_all + level_cat_sc].groupby(level_cat_sc).sum()
        else:
            out = agg_data[PE_all + level_cat_sc].groupby(level_cat_sc).count()
    if level == 'Component':
        agg_data = data[CO_all + level_cat_sc + ['Unique Component Number']].drop_duplicates()
        if sum:
            out = agg_data[CO_all + level_cat_sc].groupby(level_cat_sc).sum()
        else:
            out = agg_data[CO_all + level_cat_sc].groupby(level_cat_sc).count()
        #out = data[CO_all + [cat, level_sc]].groupby([cat]).sum()
    if level == 'max':
        # For max create separate frames for max_AI projects, PEs and components to merge them later and avoid double-counting.
        # For example PEs in data_pe are not counted in the Projects in data_pr
        data_pe = data.loc[data['PE_' + science[:]] == 1, PE_all + [cat, 'PE Number']].drop_duplicates()
        data_pr = data.loc[(data['Project_' + science[:]] == 1) & (data['PE_' + science[:]] == 0), PR_all +
                           [cat, 'Unique Project Number']].drop_duplicates()
        data_com = data.loc[(data['Component_' + science[:]] == 1) & (data['PE_' + science[:]] == 0) &
                           (data['Project_' + science[:]] == 0), CO_all + [cat,'Unique Component Number']].drop_duplicates()
        # merge max AI definition: PE + Projects + Components
        data_max = data_pe.append(data_pr.append(data_com))
        # calculate total costs for Max AI for the relevant set of keywords
        data_max['Total Max Cost'] = data_max.loc[:, ['PE Cost Total_calc', 'Project Cost Total_calc',
                                                      'Component Cost Total_calc']].sum(axis=1)
        for year in range(2018, 2021):
            data_max[f'Max {year} Cost'] = data_max.loc[:, [f'PE Cost FY {year}', f'Project Cost FY {year}',
                                                     f'Component Cost FY {year}']].sum(axis=1)
        for year in range(2021, 2025):
            data_max[f'Max {year} Cost'] = data_max.loc[:, [f'PE Cost FY {year}', f'Project Cost FY {year}']].sum(axis=1)
        # export aggregated table
        out = data_max[[cat, "Total Max Cost"]+[f"Max {year} Cost" for year in range(2018,2025)]].groupby([cat]).sum()
    return out, outname


def get_top20_projects():
    # test the function above
    print("Test of the top20 function")
    print(top20( 'PE','AI', 'Research Category', 'Advanced', 0))
    # set relevant categories for aggregation
    science = ['hum_mach','AI', 'autonomy', 'autonomy_other', 'ai_rob', 'rob_hum_mach', 'autonomy_ai']
    level = ['PE', 'Project', 'Component']
    res_cat = ['Advanced', 'Basic', 'Applied']
    ser_cat = ['USAF', 'Army', 'DARPA', 'Navy']
    # regular top 20 and export the to file top20.xlsx
    with pd.ExcelWriter('Data/top20.xlsx') as writer:
        for s in science:
            for l in level:
                for r in res_cat:
                    tab, name = top20(l, s, 'Research Category', r)
                    print(name)
                    tab.to_excel(writer, sheet_name=name)
                for ser in ser_cat:
                    tab, name = top20(l, s, 'Service', ser)
                    tab.to_excel(writer, sheet_name=name)
    # regular top 20 by the number of keywords
    with pd.ExcelWriter('Data/top20_intensity.xlsx') as writer:
        for s in science:
            for l in level:
                for r in res_cat:
                    tab, name = top20(l, s, 'Research Category', r, 1)
                    print(name)
                    tab.to_excel(writer, sheet_name=name)
                for ser in ser_cat:
                    tab, name = top20(l, s, 'Service', ser, 1)
                    tab.to_excel(writer, sheet_name=name)
    return

####### Replicate tables for Philippe:
# Calculate total costs by difference aggregation levels


def export_pivot_tables():
    print("Test Summary statistics table")
    print(sum_tab('Project',  'rob_hum_mach', 'Service', False))
    # relevant project levels and categories for aggregation
    science = ['rob_hum_mach','hum_mach','AI', 'autonomy', 'autonomy_other', 'ai_rob', 'autonomy_ai', 'autonomy_AND_AI']
    level = ['PE', 'Project', 'Component', 'max']
    cat_list = ['Service','Research Category']
    # sum tables will do sum, others will count
    sumlist = [False, True]
    for s in science:
        with pd.ExcelWriter('Data/' + s + '_sum_exp.xlsx') as writer:
            for l in level:
                for r in cat_list:
                    for m in sumlist:
                        if (((s == 'rob_hum_mach') & (l == 'max')) | ((m == 'count') & (l == 'max'))):
                            continue
                        tab, name = sum_tab(l, s, r, m)
                        tab.to_excel(writer, sheet_name=name)
    return

def upload_data_and_clean():
    # upload results to GCP
    upload_blob('cset_financial_data', 'Data/sum_exp.xlsx', 'sum_tables.csv')
    upload_blob('cset_financial_data', 'Data/top20_intensity.xlsx', 'top20_intensity.csv')
    upload_blob('cset_financial_data', 'Data/top20.xlsx', 'top20.csv')
    upload_blob('cset_financial_data', 'Data/Updated_master.csv', 'Updated_master.csv')
    upload_blob('cset_financial_data', 'Data/AI_rob.xlsx', 'AI_rob.xlsx')
    upload_blob('cset_financial_data', 'Data/autonomy_other.xlsx', 'autonomy_other.xlsx')
# delete files from the hard drive:
#    for f in ['Data/sum_exp.xlsx', 'Data/top20_intensity.xlsx', 'Data/top20.xlsx','Data/MASTER.csv',
#              'Data/Updated_master.csv', 'Data/AI_keywords.csv']:
#        os.remove(f)
    return




def get_project_tree(target, df):
    # useful columns we need to build a tree
    #  build PE level
    columns_u = ['Project_' + target , 'Component_' + target , 'PE_' + target, 'PE Number', 'PE Name', 'PE Description', 'Service',
                 'Research Category', 'Unique Project Number', 'Project Description', \
                 'Project Name', 'Unique Component Number', 'Component Title', 'Component Description']
    PE = df.loc[df['PE_' + target] == 1, columns_u].drop_duplicates()
    Project = df.loc[df['Project_' + target] == 1, columns_u].drop_duplicates()
    Component = df.loc[df['Component_' + target] == 1, columns_u].drop_duplicates()
    # count projects in PEs
    T_pr = PE.loc[PE['Project_' + target] == 1, ['Unique Project Number', 'PE Number']].groupby(
        ['PE Number']).nunique(). \
        rename(columns={'Unique Project Number': target + ' Proj Count'})[[target + ' Proj Count']]
    # count all projects in target PEs
    All_pr = PE[['Unique Project Number', 'PE Number']].groupby(['PE Number']).nunique().rename(
        columns={'Unique Project Number': 'All Proj Count'})[['All Proj Count']]
    # collect data to PE output sheet
    PE = PE.merge(T_pr, how='left', right_index=False, left_on=['PE Number'],
         right_on=['PE Number']).merge(All_pr, how='left', right_index=False, left_on=['PE Number'],
         right_on=['PE Number'])
    # count components for target PEs
    T_comp = PE.loc[PE['Component_' + target] == 1, ['Unique Component Number', 'PE Number']].groupby(
        ['PE Number']).nunique().rename(columns={'Unique Component Number': target + ' Component Count'})[
        [target + ' Component Count']]
    All_comp = PE[['Unique Component Number', 'PE Number']].groupby(['PE Number']).nunique().rename(
        columns={'Unique Component Number': 'All Component Count'})[['All Component Count']]
    # merge component counts with PE data:
    PE = PE.merge(T_comp, how='left', right_index=False, left_on=['PE Number'], right_on=['PE Number']).\
        merge(All_comp, how='left', right_index=False, left_on=['PE Number'],right_on=['PE Number'])
    # build component level. This is the lower level that does not have any branches.
    # count target components in the target projects
    T_comp = Project.loc[Project['Component_' + target] == 1, ['Unique Project Number', 'Unique Component Number']].\
        groupby(['Unique Project Number']).nunique().\
        rename(columns={'Unique Component Number': target + ' Component Count'})[[target + ' Component Count']]
    # count all components in the target projecs
    All_comp = Project[['Unique Project Number', 'Unique Component Number']].groupby(['Unique Project Number']).\
        nunique().rename(columns={'Unique Component Number': 'All Component Count'})[['All Component Count']]
    Project = Project.merge(T_comp, how='left', right_index=False, on=['Unique Project Number']).merge(All_comp,
        how='left', right_index=False,on=['Unique Project Number'])
    filename = 'Data/' + target + '.xlsx'
    with pd.ExcelWriter(filename) as writer:
        PE.to_excel(writer, sheet_name='PE_' + target)
        Project.to_excel(writer, sheet_name='Project_' + target )
        Component.to_excel(writer, sheet_name='Component_' + target)
    return

# Run the functions
if __name__ == "__main__":
     # you could comment this out if you wanted to use files you've already downloaded
     data, cost_list, autonomy_list, AI_list, robotics_list, other_list, hum_mach_list, autonomy_other_list, ai_rob_list, \
     autonomy_other_ai_list, autonomy_rob_ai_other_list, autonomy_rob_ai_list, autonomy_ai_list = download_data()
     # your other functions
     data = identify_ai_projects(data)
     data, PE_all, PR_all , CO_all = calculate_costs(data)
     get_top20_projects()
     export_pivot_tables()
     #get_project_tree('ai_rob',  data)
     #get_project_tree('autonomy_other', data)
     #get_project_tree('autonomy_ai', data)
     #get_project_tree('autonomy_AND_AI', data)
     upload_data_and_clean()


#### Test
# import os
# import pandas as pd
# os.getcwd()
# os.chdir('../Ilya_Support_Misc/LAWS')
# df = pd.read_csv('Data/Updated_master.csv')
# #
#


#12/13 PEs, 51/62 projects, and
#255/287 components classified as both autonomy and AI.