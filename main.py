# Read files in async way from web and save to disk
from typing import List, Tuple
import os
import numpy as np
import asyncio
import aiohttp
from aiohttp import client_exceptions
import aiofiles
# import aiofile
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from tqdm.asyncio import tqdm
import argparse
import shutil
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


parser = argparse.ArgumentParser()

parser.add_argument('--num_async_processes', default=3, type=int, help="Number of concurrent processes")
parser.add_argument('--num_parallel_processes', default=cpu_count() // 2, type=int, help="Number of parallel processes")
parser.add_argument('--input_file', required=True, type=str, help="Input filename with links to crops")

args = parser.parse_args()
args = vars(args)

DATA_FOLDER = "data"
shutil.rmtree(DATA_FOLDER, ignore_errors=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# Global variables
# list with all skus
sku_list: List = []
# list with triplets (line number, url, sku)
crops: List[Tuple[int, str, str]] = []


async def download_files(*arg):
    """
    async coroutine to download images and save them to a local file storage
    :param arg: an array of triplets (line_number, url, sku)
    :return:
    """
    # create semaphore for the process
    urls = arg[0]
    sema = asyncio.Semaphore(args['num_async_processes'])

    async def fetch_file(url_line: Tuple):
        line_number, url, sku = url_line
        fname = f"{sku}_{url.split('/')[-1]}"
        async with sema, aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=5 * 60) as resp:
                    if resp.status != 200:
                        logging.error(f'Houston, we have a problem with line {line_number}{url}\n')
                        data = None
                    else:
                        data = await resp.read()
            except aiohttp.client_exceptions.InvalidURL as e:
                logging.error(f'Houston, we have a problem with line {line_number}n{url}\n')
                logging.error(f'Exception {e}')
                data = None
            except asyncio.exceptions.TimeoutError as e:
                logging.error(f'Houston, we have a problem with line {line_number}n{url}\n')
                logging.error(f'Exception {e}')
                data = None
            except aiohttp.client_exceptions.ClientConnectorError as e:
                logging.error(f'Houston, we have a connection problem. Looks like we ISP has blocked us.')
                logging.error(f'Exception {e}')
                data = None

        if data:
            async with aiofiles.open(
                    os.path.join(DATA_FOLDER, sku, fname), "wb"
            ) as outfile:
                await outfile.write(data)

    tasks = [asyncio.create_task(fetch_file(url_line)) for url_line in urls]
    for marker in tqdm.as_completed(tasks):
        await marker


def download_files_sync_wrapper(*args):
    """
    Just a synchronous routine to start an async process
    :param args:
    :return:
    """
    asyncio.run(download_files(args[0]))


if __name__ == '__main__':
    # read raw data
    with open(args['input_file'], 'r') as f:
        raw_lines = f.readlines()

    # process raw data and split into url and sku
    for i, line in enumerate(raw_lines):
        try:
            _, url, sku = line.split(',')
            # remove \n at the end
            sku = sku[:-1]
            # append line number, url and sku
            # check if url has a valid structure
            if 'https://' in url:
                crops.append((i, url, sku))
            if sku not in sku_list:
                sku_list.append(sku)
        except ValueError as e:
            logging.error(f'Error in file - line # {i}. Skipping.')

    del raw_lines

    logging.info(f'Start processing {len(sku_list)} skus with {len(crops)} links.')
    logging.info(f'An average of {int(len(crops)/len(sku_list))} crops per SKU.')

    # make all dirs for skus
    for sku in sku_list:
        os.makedirs(os.path.join(DATA_FOLDER, sku), exist_ok=True)

    # split all data into num_parallel_processes and process each one asynchronously
    borders = np.linspace(0, len(crops), args['num_parallel_processes'] + 1, dtype=int)
    # create a pool
    with Pool(args['num_parallel_processes']) as pool:
        async_result = [pool.apply_async(download_files_sync_wrapper,
                                         args=(crops[b:borders[pos + 1]],))
                        for pos, b in enumerate(borders[:-1])]

        async_multi = [res.get() for res in async_result]
        print(async_multi)

    # # download files
    # asyncio.run(download_files(crops))
