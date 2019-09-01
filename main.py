import textract
import subprocess
import os
import re
import sys
from collections import OrderedDict
from fuzzysearch import find_near_matches
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl
import pandas as pd

# directory names
pdf_directory = 'oncopanel-reports-to-scan/'
text_directory = pdf_directory + 'text-files/'
xl_directory = 'oncopanel-spreadsheets.xlsx'
genes_directory = '447-genes.txt'
xl_sheet1 = 'Report Data'

# text format of header
headers = ['Brigham and Women’s Hospital', 'Molecular Diagnostics Laboratory', 'MOLECULAR DIAGNOSTICS REPORT',
           'Page x of y', 'Accession: {} Patient Name: {}']

# search parameters
ol_l_dist = 5  # Approximity of one line search on pdf text
bl_l_dist = 1  # Approximity of block search on pdf text
header_l_dist = 3  # Approximity of header search on pdf text
def_empty = 'None'  # What to enter on cell if info is not on pdf
delimiter = '\n'
# keywords and column names for extraction
keyword_to_column = OrderedDict([('Accession numbers on blocks submitted - ', 'Accession Number'),
                                 ('Accession No.: ', 'BL Accession Number'),
                                 ('Patient Name: ', 'Patient Name'),
                                 ('Unit Number(s): ', 'Unit Number'),
                                 ('Birth Date:', 'Birth Date'),
                                 ('Age Sex: ', 'Sex'),
                                 ('Test Performed - ', 'Test Performed'),
                                 ('Test Description - ', 'Test Description'),
                                 ('Original Pathologic Diagnosis - ', 'Original Pathologic Diagnosis'),
                                 ('Estimated percentage of neoplastic cells in submitted specimen - ',
                                  'Neoplastic Cells (%)'),
                                 ('DNA VARIANTS:', 'Aligned Reads'),
                                 ('aligned, high quality reads for this specimen with a mean of ',
                                  'Mean Reads Across Targeted Exons'),
                                 ('reads across all targeted exons and ', 'Exons with >30 Reads (%)'),
                                 ('Tier 1 variants:', 'Tier 1 Variants'),
                                 ('Tier 2 variants:', 'Tier 2 Variants'),
                                 ('Tier 3 variants:', 'Tier 3 Variants'),
                                 ('Tier 4 variants:', 'Tier 4 Variants'),
                                 ('Tier 5 variants:', 'Tier 5 Variants'),
                                 ('COPY NUMBER VARIATIONS:', 'Copy Number Variations'),
                                 ('CHROMOSOMAL REARRANGEMENT: ', 'Chromosomal Rearrangement'),
                                 ('INTERPRETATION', '')])
gene_columns = ['Number of Mutations', 'DNA Change', 'Protein Change', 'Exon', '% Reads', 'CNV Type', 'CNV Locus']

# index name
df_index = 'Accession Number'


def main():
    # open the Excel file and the sheet (we want to make sure something's saved!)
    xl_df = pd.read_excel(xl_directory, sheet_name=xl_sheet1)

    # get columns for 447 genes
    genes = read_genes_file(genes_directory)

    # create the dataframe for scanned data
    df_columns = list(keyword_to_column.values())[:-1]
    df_columns.insert(0, "Filename")
    for gene in genes:
        for gene_column in gene_columns:
            df_columns.append('{} {}'.format(gene, gene_column))
    new_df = pd.DataFrame(columns=df_columns).set_index(df_index)

    # iterate through all pdf files in the pdf directory
    pdfs = [filename for filename in os.listdir(os.getcwd() + '/' + pdf_directory) if filename.endswith(".pdf")]
    pdfs.sort()
    for index, filename in enumerate(pdfs):
        print("({}/{}) Working with {}".format(str(index+1), str(len(pdfs)), filename))
        text_file_dir = text_directory + filename[:-4:] + '.txt'
        # check if the file has been scanned before
        text_exists = False
        # access text file and read contents
        if os.path.exists('./' + text_file_dir):
            with open('./' + text_file_dir, 'r') as file:
                text = file.read()
                file.close()
                text_exists = True
            print('\tScan found in text directory')
        # scan the pdf
        else:
            print("\tScanning...", end='')

            # ocrmypdf branch
            #print('ocrmypdf --output-type pdf --deskew --clean --sidecar \"' + text_file_dir + '\" \"' + pdf_directory + filename + '\" \"' + pdf_directory + filename + '\"')
            subprocess.call('ocrmypdf --output-type pdf --force-ocr --deskew --clean --sidecar \"' + text_file_dir + '\" \"' + pdf_directory + filename + '\" \"' + pdf_directory + filename + '\"', shell=True)
            with open('./' + text_file_dir, 'r') as file:
                text = file.read()
                file.close()
            # end ocrmypdf branch

            # textract branch
            # text = textract.process('.' + pdf_directory + filename, method='tesseract', language='eng').decode()
            # end of textract branch
            print("completed")

        print("\tScraping...", end='')
        data_index, data_columns, text = scraper(text, genes, filename)
        # add columns to DataFrame
        new_df.loc[data_index] = data_columns
        print('completed')

        # write the text file
        if not text_exists:
            print("\tWriting text file...", end='')
            with open('./' + text_file_dir, "w+") as file:
                file.write(text)
                file.close()
            print("completed")

    # Excel insertion starts
    print("Writing Excel file...", end='')
    #xl_df = xl_df.append(new_df, sort=False).drop_duplicates()
    xl_df = new_df
    writer = pd.ExcelWriter(xl_directory, engine='xlsxwriter')
    xl_df.to_excel(writer, xl_sheet1)
    writer.save()
    writer.close()
    print("completed")


def scraper(text, genes, filename):
    for_index = def_empty
    for_columns = {"Filename": filename}
    removed = False
    for index, (term, column) in enumerate(keyword_to_column.items()):
        for_result = def_empty
        # one line searches come here
        if index < list(keyword_to_column.keys()).index('DNA VARIANTS:'):
            for_result = ol_search(text, term)
            # apply header remover once patient name and
            # accession number are done
            if 'BL Accession Number' in for_columns and 'Patient Name' in for_columns and not removed:
                text = header_remover(text, for_columns)
                removed = True
            if term == 'Age Sex: ':
                if find_near_matches_r('Male', for_result, max_l_dist=1):
                    for_result = 'Male'
                elif find_near_matches_r('Female', for_result, max_l_dist=1):
                    for_result = 'Female'
        # block searches
        elif index < len(keyword_to_column) - 1:
            for_result = block_search(text, index)
            if for_result is None:
                for_result = def_empty
            # print("{} {}".format(term, list(keyword_to_column.items())[index+1][0]))
            if index < list(keyword_to_column.keys()).index('Tier 1 variants:'):
                numbers = [s for s in re.split(' |%', for_result) if s.isdigit()]
                if numbers:
                    for_result = str(numbers[0])
            # record result of search
        if column == df_index:
            for_index = for_result
        elif index < len(keyword_to_column) - 1:
            for_columns[column] = for_result
    # gene scraping

    CNVs = for_columns['Copy Number Variations'].split(delimiter)
    TVs = [TV for i in range(1, 6) for TV in for_columns['Tier {} Variants'.format(str(i))].split(delimiter)]
    for gene in genes:
        CNV_outputs = {'CNV Type': def_empty, 'CNV Locus': def_empty}
        TV_outputs = {'DNA Change': def_empty, 'Protein Change': def_empty, 'Exon': def_empty, '% Reads': def_empty}
        for CNV in CNVs:
            if gene in CNV:
                CNV_list = CNV.split()
                try:
                    CNV_outputs['CNV Type'] = commadd(CNV_outputs['CNV Type'], cnv_type(CNV))
                    CNV_outputs['CNV Locus'] = commadd(CNV_outputs['CNV Locus'], CNV_list[0])
                except IndexError:
                    pass

        for TV in TVs:
            if gene in TV:
                try:
                    TV_outputs['DNA Change'] = commadd(TV_outputs['DNA Change'], TV[TV.find(gene) + len(gene)::].split()[0])
                    TV_outputs['Protein Change'] = commadd(TV_outputs['Protein Change'], TV[TV.find('(') + 1:TV.find(')'):])
                    TV_outputs['Exon'] = commadd(TV_outputs['Exon'], TV[TV.find('exon') + len('exon')::].split()[0])
                    reads_search = find_near_matches_r('in ab% of xyz reads', TV, max_l_dist=7)
                    if reads_search:
                        TV_outputs['% Reads'] = commadd(TV_outputs['% Reads'], TV[reads_search[0].start:reads_search[0].end:])
                except IndexError:
                    continue

        for_columns['{} Number of Mutations'.format(gene)] = 0 if TV_outputs['DNA Change'] == def_empty \
            else len(TV_outputs['DNA Change'].split(','))
        for TV_att in TV_outputs.keys():
            for_columns[gene + ' ' + TV_att] = TV_outputs[TV_att]
        for CNV_att in CNV_outputs.keys():
            for_columns[gene + ' ' + CNV_att] = CNV_outputs[CNV_att]

    return for_index, for_columns, text


def header_remover(text, for_columns):  # Removes residual headers
    newtext = text
    for header in headers:
        keyword = header.format(for_columns['BL Accession Number'], for_columns['Patient Name'])
        search_result = find_near_matches_r(keyword, newtext, max_l_dist=header_l_dist)
        while len(search_result) != 0:
            newtext = newtext[:search_result[0].start] + newtext[search_result[0].end:]
            search_result = find_near_matches_r(keyword, newtext, max_l_dist=header_l_dist)
    return newtext


def ol_search(text, keyword):  # Returns one line after keyword
    searchresult = find_near_matches_r(keyword, text, max_l_dist=ol_l_dist)
    if not searchresult:
        return def_empty
    else:
        startindex = searchresult[0].end
        endindex = text[startindex::].find('\n') + startindex
        return text[startindex:endindex:].lstrip().rstrip()


def block_search(text, key_index):  # removes the stuff in between
    keys_list = list(keyword_to_column.keys())
    searchresult = find_near_matches_r(keys_list[key_index], text, max_l_dist=bl_l_dist)
    if searchresult is None or len(searchresult) == 0:
        searchresult = find_near_matches_r(keys_list[key_index].capitalize(), text, max_l_dist=bl_l_dist)
        if searchresult is None or len(searchresult) == 0:
            return def_empty
    else:
        startindex = searchresult[0].end
        endsearch = []
        i = 0
        while not endsearch and key_index < len(keys_list) and i < 2:
            key_index += 1
            i += 1
            endsearch = find_near_matches_r(keys_list[key_index], text[startindex::], max_l_dist=bl_l_dist)
            endsearch2 = find_near_matches_r(keys_list[key_index].capitalize(), text, max_l_dist=bl_l_dist)
            if endsearch2 is not None and len(endsearch2) > 0:
                if endsearch is not None and len(endsearch) > 0:
                    if endsearch[0].start > endsearch2[0].start:
                        endsearch = endsearch2
                else:
                    endsearch = endsearch2
            if len(keys_list) > key_index + 1:
                nextsearch = find_near_matches_r(keys_list[key_index + 1], text[startindex::], max_l_dist=bl_l_dist)
                if endsearch is not None and len(endsearch) > 0 and nextsearch is not None and len(nextsearch) > 0:
                    if nextsearch[0].start < endsearch[0].start:
                        endsearch = nextsearch
        if not endsearch:
            return def_empty
        else:
            endindex = endsearch[0].start + startindex
        return text[startindex:endindex:].lstrip().rstrip().replace('\n', delimiter)


# rearranges the search result by l_dist, the way it should've been :P
def find_near_matches_r(keyword, text, max_l_dist=1):
    result = find_near_matches(keyword, text, max_l_dist=max_l_dist)
    if len(result) > 0:
        result.sort(key=lambda x: x.dist)
    return result


def read_genes_file(path):
    return_list = []
    try:
        with open('./' + path, "r") as file:
            text = file.read()
            file.close()
        return_list = re.split(' |,', text)
    except IOError:
        print('Gene file not found at ' + path)
    return return_list


def commadd(sum, addthis):
    if sum == def_empty:
        return addthis
    else:
        return sum + ', ' + addthis


def cnv_type(CNV):
    cnv_types = ['Single', 'Deep', 'Low', 'High']
    for index, cnv_type in enumerate(cnv_types):
        if find_near_matches(cnv_type, CNV, max_l_dist=1):
            return str(index + 1)
    return str(0)


main()
