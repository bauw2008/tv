#!/usr/bin/env python3
import aiohttp
import asyncio
import argparse
import json
import logging
import os
import re
import sys
import urllib.parse
from typing import List, Dict, Optional, Tuple, Any, Set

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(task_name)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
# Add a custom adapter to inject task_name into logs
class TaskLogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        task_name = self.extra.get('task_name', 'Main')
        # Ensure task_name doesn't accidentally overwrite existing format args
        if 'task_name' not in kwargs:
             kwargs['extra'] = {'task_name': task_name}
        else: # Avoid conflict if logger format string uses %(task_name)s
             kwargs['extra'] = kwargs.get('extra', {})
             kwargs['extra']['task_name'] = task_name
        return f"[{task_name}] {msg}", kwargs

logger = TaskLogAdapter(logging.getLogger(__name__), {'task_name': 'Main'})


# --- Default Configuration ---
class ConfigDefaults:
    REQUEST_TIMEOUT = 5
    MAX_RETRIES = 2
    RETRY_DELAY = 2
    ATTRIBUTES_TO_REMOVE = ["tvg-logo", "tvg-id"]
    DEFAULT_CONFIG_STRUCTURE = {
        "global_settings": {
            "request_timeout": REQUEST_TIMEOUT,
            "max_retries": MAX_RETRIES,
            "retry_delay": RETRY_DELAY
        },
        "tasks": [
            {
                "name": "Default Merge Filter Task",
                "enabled": True,
                "urls": ["https://raw.githubusercontent.com/vbskycn/iptv/refs/heads/master/tv/iptv4.m3u"],
                "processing_mode": "merge_filter",
                "attributes_to_remove": ATTRIBUTES_TO_REMOVE,
                "output_files": {
                    "https_output": "filtered_https_only.m3u",
                    "http_valid_output": "filtered_http_only_valid.m3u"
                },
                "filter_rules": {
                     "https_exclude_tvgname_contains": []
                }
            }
        ]
    }

# --- Configuration Loading ---
def load_config(config_file: str = "config.json") -> Dict:
    """从JSON配置文件加载配置，失败时使用默认结构"""
    config_data = ConfigDefaults.DEFAULT_CONFIG_STRUCTURE.copy() # Start with default structure
    if os.path.exists(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                user_config = json.load(f)

            # Merge global settings (user settings override defaults)
            if "global_settings" in user_config and isinstance(user_config["global_settings"], dict):
                 config_data["global_settings"].update(user_config["global_settings"])

            # Replace or append tasks (here we replace default task list if user provides any)
            if "tasks" in user_config and isinstance(user_config["tasks"], list):
                 config_data["tasks"] = user_config["tasks"]
                 # Optional: Add validation/default filling for each task here if needed

            logger.info(f"已加载配置文件: {config_file}")
            return config_data
        except (IOError, json.JSONDecodeError) as e:
            logger.warning(f"加载配置文件 {config_file} 失败，将使用默认配置: {e}")
            return ConfigDefaults.DEFAULT_CONFIG_STRUCTURE # Return default structure on error
    else:
        logger.info("未找到配置文件，使用默认配置。")
        return ConfigDefaults.DEFAULT_CONFIG_STRUCTURE

# --- Network Operations ---
async def fetch_m3u_content_async(url: str, session: aiohttp.ClientSession, timeout: int) -> Optional[str]:
    """异步获取单个 M3U URL 的内容"""
    try:
        # logger.debug(f"Fetching URL: {url}")
        async with session.get(url, timeout=timeout * 2) as response: # Give more time for initial download
            response.raise_for_status()
            content = await response.text(encoding='utf-8', errors='ignore')
            # logger.debug(f"Successfully fetched: {url}")
            return content
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"获取 M3U 文件时发生错误 {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"获取 M3U 文件时发生未知错误 {url}: {e}")
        return None

async def is_url_accessible_async(url: str, session: aiohttp.ClientSession, config: Dict, cache: Dict[str, bool]) -> bool:
    """
    异步检查URL是否可访问（返回2xx状态码）。
    使用 *全局配置* 中的超时、重试次数和延迟。
    (Function unchanged, uses global settings from loaded config)
    """
    if url in cache:
        return cache[url]

    global_settings = config.get("global_settings", {})
    max_retries = global_settings.get("max_retries", ConfigDefaults.MAX_RETRIES)
    request_timeout = global_settings.get("request_timeout", ConfigDefaults.REQUEST_TIMEOUT)
    retry_delay = global_settings.get("retry_delay", ConfigDefaults.RETRY_DELAY)
    is_accessible = False

    for attempt in range(max_retries):
        current_try = attempt + 1
        # HEAD first
        try:
            async with session.head(url, timeout=request_timeout, allow_redirects=True, ssl=False) as response:
                is_accessible = 200 <= response.status < 300
                if is_accessible: break
                if response.status != 405:
                    # logger.debug(f"HEAD failed (non-405) {url}: Status {response.status}. Try {current_try}/{max_retries}")
                    pass # Continue to GET try or retry delay

        except (aiohttp.ClientResponseError) as e_head:
            # logger.debug(f"HEAD HTTP Error {url}: Status {e_head.status}. Try {current_try}/{max_retries}")
            if e_head.status != 405: pass
        except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError) as e_head:
            # logger.debug(f"HEAD Connect/Timeout/Payload Error {url}: {type(e_head).__name__}. Try {current_try}/{max_retries}. Try GET...")
            pass
        except Exception as e_head:
            logger.warning(f"HEAD Unexpected Error {url}: {e_head}. Try {current_try}/{max_retries}. Try GET...")

        # GET if HEAD failed or was 405
        if not is_accessible:
            try:
                async with session.get(url, timeout=request_timeout, allow_redirects=True, ssl=False) as response:
                    try:
                        await asyncio.wait_for(response.content.readany(), timeout=request_timeout / 2)
                    except (asyncio.TimeoutError, Exception):
                        pass # Ignore read errors for validation, just check status
                    is_accessible = 200 <= response.status < 300
                    if is_accessible: break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e_get:
                # logger.debug(f"GET Request Error {url}: {type(e_get).__name__}. Try {current_try}/{max_retries}")
                pass
            except Exception as e_get:
                 logger.warning(f"GET Unexpected Error {url}: {e_get}. Try {current_try}/{max_retries}")

        if not is_accessible and attempt < max_retries - 1:
            # logger.debug(f"URL {url} attempt {current_try} failed, retrying in {retry_delay}s...")
            await asyncio.sleep(retry_delay)

    cache[url] = is_accessible
    if not is_accessible:
        logger.info(f"URL {url}最终判定为无法访问 after {max_retries} attempts.")
    return is_accessible

async def check_urls_async(urls: List[str], config: Dict, cache: Dict[str, bool]) -> List[bool]:
    """批量异步检查URL列表的可访问性"""
    global_settings = config.get("global_settings", {})
    request_timeout = global_settings.get("request_timeout", ConfigDefaults.REQUEST_TIMEOUT)
    # Increase limits cautiously
    conn = aiohttp.TCPConnector(limit_per_host=20, limit=100, ssl=False)
    timeout = aiohttp.ClientTimeout(total=request_timeout * 1.5) # Overall timeout slightly larger
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        tasks = [is_url_accessible_async(url, session, config, cache) for url in urls]
        results = await asyncio.gather(*tasks)
        return results

# --- M3U Processing Helpers ---
def remove_attributes(extinf_line: str, attributes_to_remove: List[str]) -> str:
    """从EXTINF行中完全移除指定的属性及其值 (case-insensitive attribute match)"""
    modified_line = extinf_line
    for attr in attributes_to_remove:
        # Regex: space(s) + attribute_name + = + (quoted_value | unquoted_value)
        pattern = rf'\s+{re.escape(attr)}=(?:"[^"]*"|\'[^\']*\'|[^\s,]+)'
        modified_line = re.sub(pattern, '', modified_line, flags=re.IGNORECASE)
    return modified_line.strip()

def parse_extinf_attributes(extinf_line: str) -> Dict[str, str]:
    """从EXTINF行解析键值对属性"""
    attributes = {}
    # Regex: Find key="value" or key=value patterns
    # (\S+?)=        Capture key (non-greedy, non-space) followed by =
    # (              Capture value group
    #  "(.*?)"      Capture content within double quotes (non-greedy)
    #  |            OR
    #  '(.*?)'      Capture content within single quotes (non-greedy)
    #  |            OR
    #  ([^\s",]+)   Capture unquoted value (until space, comma, or quote)
    # )
    pattern = re.compile(r'(\S+?)=(?:"(.*?)"|\'(.*?)\'|([^\s",]+))')
    matches = pattern.findall(extinf_line)
    for key, val_double, val_single, val_unquoted in matches:
        # The value is the captured group that is not empty
        value = val_double or val_single or val_unquoted
        attributes[key.lower()] = value # Store keys in lower case for consistency
    # Also capture the channel name after the last comma
    name_match = re.search(r',\s*(.*)$', extinf_line)
    if name_match:
        # Use a standard key like 'channel_name' for the part after comma
        attributes['channel_name'] = name_match.group(1).strip()
    return attributes

def is_ip_address(host: Optional[str]) -> bool:
    """检查主机名是否为IP地址（IPv4或IPv6）- Robust check"""
    if not host: return False
    # Remove port for IPv4 if present
    if ':' in host and '.' in host: # Potential IPv4:port
        host = host.split(':', 1)[0]
    # Remove brackets and port for IPv6 if present
    elif host.startswith('['):
        host = host.strip('[]')
        if ']:' in host: # Should not happen with urlparse hostname, but be safe
             host = host.split(']:', 1)[0]

    # Use ipaddress module if available (most reliable)
    try:
        import ipaddress
        ipaddress.ip_address(host)
        return True
    except ValueError: # Not a valid IP address according to the module
        return False
    except ImportError: # Fallback if ipaddress module is not present
        logger.warning("ipaddress module not found. Using regex for IP validation (less reliable).")
        # Basic Regex Check (less comprehensive)
        ipv4_pattern = r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"
        ipv6_pattern = r".*[:].*" # Very basic check for IPv6 presence
        return bool(re.fullmatch(ipv4_pattern, host) or re.match(ipv6_pattern, host))

def write_m3u_file(filename: str, header: str, lines: List[str], task_name: str = "Unknown Task") -> None:
    """Writes M3U content to a file."""
    task_logger = TaskLogAdapter(logger.logger, {'task_name': task_name})
    channel_count = (len(lines)) // 2 if lines else 0
    task_logger.info(f"正在写入文件: {filename} (共 {channel_count} 个频道)")
    try:
        output_dir = os.path.dirname(filename)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            task_logger.info(f"创建目录: {output_dir}")

        full_content = [header] + lines
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(full_content) + "\n") # Ensure trailing newline
        task_logger.info(f"文件 {filename} 保存成功！")
    except IOError as e:
        task_logger.error(f"写入文件 {filename} 时发生 IO 错误: {e}")
    except Exception as e:
        task_logger.error(f"写入文件 {filename} 时发生未知错误: {e}")


# --- Processing Tasks ---

async def process_merge_filter_task(task_config: Dict, global_settings: Dict, session: aiohttp.ClientSession, url_cache: Dict[str, bool]) -> None:
    """Processes tasks requiring merge, deduplication, filtering, and validation."""
    task_name = task_config.get("name", "Unnamed MergeFilter Task")
    task_logger = TaskLogAdapter(logger.logger, {'task_name': task_name})
    task_logger.info("任务开始执行...")

    urls_to_fetch = task_config.get("urls", [])
    if not urls_to_fetch:
        task_logger.warning("任务未配置任何 URL，跳过。")
        return

    attributes_to_remove = task_config.get("attributes_to_remove", ConfigDefaults.ATTRIBUTES_TO_REMOVE)
    output_files = task_config.get("output_files", {})
    https_output_file = output_files.get("https_output")
    http_valid_output_file = output_files.get("http_valid_output")
    filter_rules = task_config.get("filter_rules", {})
    https_exclude_cgtn = filter_rules.get("https_exclude_tvgname_contains", [])

    if not https_output_file or not http_valid_output_file:
        task_logger.error("任务配置缺少 'https_output' 或 'http_valid_output' 文件名，跳过。")
        return

    # 1. Fetch all source contents concurrently
    request_timeout = global_settings.get("request_timeout", ConfigDefaults.REQUEST_TIMEOUT)
    fetch_tasks = [fetch_m3u_content_async(url, session, request_timeout) for url in urls_to_fetch]
    results = await asyncio.gather(*fetch_tasks)
    combined_content = "\n".join(filter(None, results)) # Combine non-None results

    if not combined_content:
        task_logger.error("未能从任何指定的 URL 获取内容，任务终止。")
        return

    # 2. Parse, Deduplicate (by URL), and Preliminarily Filter
    task_logger.info("开始解析、去重和初步过滤...")
    lines = combined_content.splitlines()
    unique_channels: Dict[str, str] = {} # Key: stream_url, Value: original_extinf_line
    m3u_header = "#EXTM3U" # Default header
    if lines and lines[0].strip().startswith("#EXTM3U"):
        m3u_header = lines[0].strip()
        lines = lines[1:]

    current_extinf: Optional[str] = None
    processed_count = 0
    duplicate_count = 0

    for line in lines:
        line = line.strip()
        if not line: continue

        if line.startswith("#EXTINF"):
            current_extinf = line
        elif current_extinf and not line.startswith("#"):
            stream_url = line
            processed_count += 1
            if stream_url not in unique_channels:
                unique_channels[stream_url] = current_extinf
            else:
                duplicate_count += 1
            current_extinf = None # Reset for next pair
        # Ignore other lines for now (#EXTVLCOPT etc.) or handle if needed

    task_logger.info(f"初步解析完成。总处理条目: {processed_count}, 唯一频道URL: {len(unique_channels)}, 发现重复条目: {duplicate_count}")

    # 3. Process Unique Channels: Remove attributes, filter, classify
    https_output_lines: List[str] = []
    http_to_check: List[Tuple[str, str]] = [] # Store (modified_extinf, url)
    http_urls_to_check_set: Set[str] = set() # Unique URLs needing check
    http_ip_discarded_count = 0
    https_cgtn_discarded_count = 0
    https_kept_count = 0
    http_candidate_count = 0

    for stream_url, original_extinf in unique_channels.items():
        # Remove attributes first
        modified_extinf = remove_attributes(original_extinf, attributes_to_remove)

        try:
            parsed_url = urllib.parse.urlparse(stream_url)
            scheme = parsed_url.scheme.lower() if parsed_url.scheme else ''
            host = parsed_url.hostname
        except ValueError:
             task_logger.warning(f"无法解析的URL，丢弃: {stream_url}")
             continue

        if scheme == "https":
            # Apply CGTN filter
            extinf_attrs = parse_extinf_attributes(original_extinf) # Parse original for tvg-name
            tvg_name = extinf_attrs.get("tvg-name", "").lower()
            excluded = False
            if https_exclude_cgtn:
                for keyword in https_exclude_cgtn:
                    if keyword.lower() in tvg_name:
                        # task_logger.debug(f"丢弃 HTTPS (tvg-name filter '{keyword}'): {stream_url}")
                        https_cgtn_discarded_count += 1
                        excluded = True
                        break
            if not excluded:
                https_output_lines.extend([modified_extinf, stream_url])
                https_kept_count += 1
        elif scheme == "http":
            if host and is_ip_address(host):
                # task_logger.debug(f"丢弃 HTTP (IP地址): {stream_url}")
                http_ip_discarded_count += 1
            else:
                # task_logger.debug(f"候选 HTTP (非IP): {stream_url}")
                http_to_check.append((modified_extinf, stream_url))
                http_urls_to_check_set.add(stream_url)
                http_candidate_count += 1
        # else: log warning for other schemes if necessary

    task_logger.info(f"分类完成。HTTPS频道: {https_kept_count} (已移除含 CGTN 等: {https_cgtn_discarded_count}), HTTP IP频道(已丢弃): {http_ip_discarded_count}, HTTP候选频道(待检查): {http_candidate_count}")

    # 4. Check HTTP URL Accessibility
    http_valid_output_lines: List[str] = []
    http_valid_count = 0
    if http_to_check:
        task_logger.info(f"开始批量检查 {len(http_urls_to_check_set)} 个唯一的 HTTP URL 的可访问性...")
        unique_http_urls = list(http_urls_to_check_set)
        # Pass global_settings dict to check_urls_async for timeout/retry config
        results = await check_urls_async(unique_http_urls, {"global_settings": global_settings}, url_cache)
        validity_map = dict(zip(unique_http_urls, results))
        task_logger.info("HTTP URL 检查完成。")

        task_logger.info("构建有效的 HTTP 频道列表...")
        for extinf, url in http_to_check:
            if validity_map.get(url, False):
                http_valid_output_lines.extend([extinf, url])
                http_valid_count += 1
            # else: task_logger.debug(f"丢弃无效 HTTP: {url}")

    task_logger.info(f"HTTP 检查和过滤完成。保留有效HTTP频道: {http_valid_count}")

    # 5. Write Output Files
    write_m3u_file(https_output_file, m3u_header, https_output_lines, task_name)
    write_m3u_file(http_valid_output_file, m3u_header, http_valid_output_lines, task_name)

    task_logger.info("任务执行完毕。")


async def process_attributes_only_task(task_config: Dict, global_settings: Dict, session: aiohttp.ClientSession) -> None:
    """Processes tasks requiring only attribute removal."""
    task_name = task_config.get("name", "Unnamed AttributesOnly Task")
    task_logger = TaskLogAdapter(logger.logger, {'task_name': task_name})
    task_logger.info("任务开始执行...")

    urls_to_fetch = task_config.get("urls", [])
    if not urls_to_fetch:
        task_logger.warning("任务未配置任何 URL，跳过。")
        return
    if len(urls_to_fetch) > 1:
        task_logger.warning("此模式仅处理第一个 URL，忽略额外的 URL。")

    source_url = urls_to_fetch[0]
    attributes_to_remove = task_config.get("attributes_to_remove", ConfigDefaults.ATTRIBUTES_TO_REMOVE)
    output_files = task_config.get("output_files", {})
    processed_output_file = output_files.get("processed_output")

    if not processed_output_file:
        task_logger.error("任务配置缺少 'processed_output' 文件名，跳过。")
        return

    # 1. Fetch source content
    request_timeout = global_settings.get("request_timeout", ConfigDefaults.REQUEST_TIMEOUT)
    content = await fetch_m3u_content_async(source_url, session, request_timeout)

    if not content:
        task_logger.error(f"未能获取内容: {source_url}，任务终止。")
        return

    # 2. Parse and Remove Attributes
    task_logger.info("开始解析并移除属性...")
    lines = content.splitlines()
    output_lines: List[str] = []
    m3u_header = "#EXTM3U" # Default header
    if lines and lines[0].strip().startswith("#EXTM3U"):
        m3u_header = lines[0].strip()
        lines = lines[1:]

    current_extinf: Optional[str] = None
    processed_count = 0

    for line in lines:
        line = line.strip()
        if not line: continue

        if line.startswith("#EXTINF"):
            # Remove attributes from this line
            current_extinf = remove_attributes(line, attributes_to_remove)
        elif current_extinf and not line.startswith("#"):
            stream_url = line
            # Add the modified extinf and original url to output
            output_lines.extend([current_extinf, stream_url])
            processed_count += 1
            current_extinf = None # Reset for next pair
        elif line.startswith("#"): # Keep other M3U tags?
             # Decide if other # lines should be kept. Add here if needed.
             # output_lines.append(line) # Example: keep all comment lines
             pass # Ignore other comment/directive lines for now
        else: # Handle non-standard lines if needed
             task_logger.warning(f"发现非标准行，忽略: {line}")
             current_extinf = None


    task_logger.info(f"处理完成。总共处理频道条目: {processed_count}")

    # 3. Write Output File
    write_m3u_file(processed_output_file, m3u_header, output_lines, task_name)

    task_logger.info("任务执行完毕。")


# --- Main Execution ---
def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="Filter and process M3U playlists based on config.")
    parser.add_argument("--config", default="config.json", help="Path to JSON configuration file.")
    return parser.parse_args()

async def main():
    """主函数入口"""
    global logger # Allow main to update the logger adapter context if needed
    args = parse_args()
    config = load_config(args.config)

    global_settings = config.get("global_settings", {})
    tasks_to_run = [task for task in config.get("tasks", []) if task.get("enabled", False)]

    if not tasks_to_run:
        logger.warning("配置文件中没有找到启用的任务。")
        return

    # Shared cache for URL accessibility checks across tasks
    url_accessibility_cache: Dict[str, bool] = {}

    # Shared session for efficiency
    conn = aiohttp.TCPConnector(limit_per_host=25, limit=150, ssl=False) # Slightly increased limits
    request_timeout = global_settings.get("request_timeout", ConfigDefaults.REQUEST_TIMEOUT)
    timeout = aiohttp.ClientTimeout(total=request_timeout * 2) # Generous total timeout for session

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        for task_config in tasks_to_run:
            task_name = task_config.get("name", "Unnamed Task")
            processing_mode = task_config.get("processing_mode")
            # Update logger context for the current task
            logger = TaskLogAdapter(logging.getLogger(__name__), {'task_name': task_name})

            if processing_mode == "merge_filter":
                await process_merge_filter_task(task_config, global_settings, session, url_accessibility_cache)
            elif processing_mode == "attributes_only":
                await process_attributes_only_task(task_config, global_settings, session)
            else:
                logger.error(f"未知的处理模式 '{processing_mode}'，跳过任务。")

    logger = TaskLogAdapter(logging.getLogger(__name__), {'task_name': 'Main'}) # Reset logger context
    logger.info("所有任务执行完毕。")

if __name__ == "__main__":
    # Setup asyncio loop policy for Windows if needed (optional)
    # if sys.platform == "win32":
    #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info("脚本被用户中断。")
        sys.exit(1)
    except Exception as e:
         logger.exception(f"脚本执行过程中发生未捕获的异常: {e}")
         sys.exit(1)
