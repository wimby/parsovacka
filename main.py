import csv
import os
import logging
from getopt import getopt, GetoptError

import sys
from tqdm import tqdm
from werkzeug.datastructures import MultiDict


def read_folder(basename, yyyy, mm):
    """
    goes through yyyy/mm/dd folder structure
    :returns files with pattern yyyymmdd_Dat.csv
    """
    dirname = os.path.join(os.path.abspath(basename), yyyy, mm)
    for dd in os.listdir(dirname):
        f = os.path.join(dirname, dd, yyyy + mm + dd + '_Dat.csv')
        if os.path.isfile(f):
            yield f


def parse_csv(filename):
    """
    parses as regular csv
    :returns lines for one bill
    """
    with open(filename, encoding='windows-1250') as csv_file:
        reader = csv.reader(csv_file, delimiter='\t', quotechar='"')
        bill_lines = []
        for line in reader:
            bill_lines.append(line)
            # document starts with DOCTR
            if line[0] == 'DOCTR':
                yield bill_lines
                bill_lines = []


def process_doc(lines):
    """
    processes lines
    :returns bill
    """
    # change structure from [[key, ?, ?, ...?]] to associative dict(key, [?, ?, ...?])
    lines = MultiDict([(line[0], line[1:]) for line in lines])
    bill = {
        'type': lines['DOCHDR'][1],
        'id': lines['RCPID'][1] if 'RCPID' in lines else None,
        'salesman': lines['RCPID'][0] if 'RCPID' in lines else None,
        'total_price': lines['TTL'][0].replace('.', ',') if 'TTL' in lines else None,
        'price_without_vat': lines['TAXI'][3].replace('.', ',') if 'TAXI' in lines else None,
        'vat_amount': lines['TAXI'][4].replace('.', ',') if 'TAXI' in lines else None,
        'adjustment': '{:.2f}'.format(sum(map(lambda x: float(x[3]), lines.getlist('ADJI')))).replace('.', ','),
        'payment_type': lines['TNDR'][0] if 'TNDR' in lines else None,
        'date': lines['RCPDT'][0] if 'RCPDT' in lines else None,
        'cash_id': lines['ECRDESCR'][0] if 'ECRDESCR' in lines else None,
        'vat_id': lines['ECRDESCR'][1] if 'ECRDESCR' in lines else None,
    }
    bill['items'] = _process_items(lines.getlist('SI'), lines.getlist('ADJI'), bill['type'] == 'REFUND'),
    return bill


def _process_items(items, adjustments, refund):
    sales = {adj[0]: adj[3] for adj in adjustments}
    processed = []
    for item in items:
        order = item[1]
        storno = item[13]
        amount = item[6].replace('.', ',')
        processed.append({
            'order': order,
            'item_id': item[2],
            'title': item[3],
            'price': item[4].replace('.', ','),
            'price_total': item[5].replace('.', ','),
            'amount': '-' + amount if storno == 'V' or refund else amount,
            'unit': item[7],
            'type': item[8],
            'sale': '-' + sales[order].replace('.', ',') if order in sales else '0',
        })
    return processed


def append_doc_to_csv(csv1, csv2, doc):
    if doc['type'] != 'SALES' and doc['type'] != 'REFUND':
        logger.info('skipping {}'.format(doc['type']))
        return

    if doc['id'] is '':
        logger.info('RCPID is empty')
        return

    csv1.write('{id};{date};{salesman};{price_without_vat};{vat_amount};{adjustment};{payment_type};{cash_id};{total_price}\n'.format(**doc))

    for item in doc['items']:
        csv2.write('{id};{item_id};{amount};{price};{sale}\n'.format(id=doc['id'], **item))


def split_datadir_arg(datadir):
    parts = datadir.split(os.path.sep)
    if len(parts) < 3:
        raise IndexError('Wrong dirname structure')

    return os.path.sep.join(parts[:-2]), parts[-2], parts[-1]


def print_help():
    print('main.py <data-directory> [--file=output-filename]')


def get_directory(argv):
    try:
        datadir = argv[1]
    except IndexError:
        print('Please provide existing directory with data for month (basename/yyyy/mm)')
        datadir = input()
    if not os.path.isdir(datadir):
        logger.exception('Invalid directory')
        sys.exit(2)
    return datadir


def main(argv):
    try:
        datadir = get_directory(argv)
        basedir, year, month = split_datadir_arg(datadir)
        opts, args = getopt(argv[2:], 'hf:', ['help', 'file='])
    except IndexError:
        logger.exception('Please provide directory with data for month (basename/yyyy/mm)')
        print_help()
        sys.exit(2)

    except GetoptError:
        print_help()
        sys.exit(2)

    outputfile = '{}_{}'.format(year, month)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit(0)
        elif opt in ('-f', '--file'):
            outputfile = arg

    outputfile_plu = outputfile + '_plu.csv'
    outputfile += '.csv'

    logger.info('Output files are "{}"'.format(outputfile))

    with open(outputfile, 'w+', encoding='utf-8') as csv1:
        with open(outputfile_plu, 'w+', encoding='utf-8') as csv2:
            processed = 0
            failed = []
            try:
                for filename in tqdm(read_folder(basedir, year, month)):
                    logger.info('started processing file: {}'.format(filename))
                    try:
                        for lines in parse_csv(filename):
                            doc = process_doc(lines)
                            append_doc_to_csv(csv1, csv2, doc)
                            processed += 1
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        failed.append(filename)
                        logger.exception(e)

                print('processed {} documents'.format(processed))
                print('failed: {}'.format(failed if failed else 0))

            except FileNotFoundError:
                logger.exception('Provided directory "{}" does not exist'.format(datadir))
                sys.exit(1)


if __name__ == '__main__':
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    main(sys.argv)
