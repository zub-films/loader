import json
import os
from multiprocessing import Process
import time
import requests
from urllib.parse import quote_plus

IMDB_CDN = "https://v2.sg.media-imdb.com/suggestion/h/"

def pars_keys(input_path: str) -> list[str]:
    top_level_keys = []
    with open(input_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        top_level_keys = list(data.keys())
    return top_level_keys

def get_only_new_keys(dir_path: str, keys: list[str]) -> list[str]:
    missing_files = []
    
    for key in keys:
        if not os.path.exists(f'{dir_path}/{key}'):
            missing_files.append(key)
    
    return missing_files

def split_list(lst, n=5):
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]

def execute_link(data: dict) -> str | None:
    d = data.get('d')
    if d is None or len(d) == 0:
        return
    
    for item in d:
        category = item.get('qid')
        image = item.get('i')
        if category != 'movie' or image is None:
            continue
        else:
            return image.get('imageUrl')
    
    return

def worker_process(process_id, data_chunk, base_path):
    for item in data_chunk:
        print(f'{process_id}# {item}')
        try:
            resp = requests.get(f'{IMDB_CDN}{item}.json')
            if resp.status_code != 200:
                continue
            link = execute_link(resp.json())
            if link is None:
                continue
            with open(f'{base_path}/{item}', 'w', encoding='utf-8') as file:
                file.write(link)
        except Exception:
            continue
        time.sleep(2)

def main(dir_path: str, input_path: str) -> None:
    # Make dir
    os.makedirs(os.path.dirname(dir_path), exist_ok=True)
    
    keys = pars_keys(input_path)
    
    undefined_links = get_only_new_keys(dir_path, keys)
    
    print(f"All: {len(keys)}\nNew: {len(undefined_links)}")
    
    processes = []
    
    for i, chunk in enumerate(split_list(undefined_links,3)):
        process = Process(target=worker_process, args=(i+1, chunk, dir_path))
        processes.append(process)
        process.start()
        print(f"Process started {i+1}")
    
    for process in processes:
        process.join()
    print("Proceses ended")

if __name__ == "__main__":
    main("./images", "output.json")