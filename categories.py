import hashlib
import json
import os
import queue
import re
import shutil
import subprocess
import threading
import tkinter as tk
import ctypes
import ctypes.wintypes
import time
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog, messagebox, ttk


VIDEO_EXTS = {".mp4", ".mkv", ".rmvb", ".avi"}
# 严格格式：可带前置数字，核心必须是 前缀+分隔符+数字
CODE_STRICT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])(?:\d{1,4})?([A-Za-z][A-Za-z0-9]{1,7}(?:-[A-Za-z0-9]{2,8})?)\s*[-_ ]\s*(\d{2,8})(?!\d)",
    re.IGNORECASE,
)
# 宽松格式：无分隔符
CODE_LOOSE_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])([A-Za-z]{3,8})\s*(\d{2,6})(?!\d)",
    re.IGNORECASE,
)
NOISE_PREFIXES = {"HHD", "KFA", "WWW", "COM", "HTTP", "HTTPS", "VIDEO", "MOVIE"}
CHUNK_SIZE = 1024 * 1024
FFPROBE_TIMEOUT_SEC = 10
MATCH_PAGE_SIZE = 40


@dataclass
class VideoInfo:
    path: Path
    size: int
    resolution: str
    bitrate: str
    duration: str = "未分析"
    md5: str = ""
    code: str = ""


class ScrollableFrame(ttk.Frame):
    def __init__(self, master, canvas_height=None):
        super().__init__(master)
        self.canvas = tk.Canvas(self, highlightthickness=0)
        if canvas_height:
            self.canvas.configure(height=canvas_height)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.inner = ttk.Frame(self.canvas)

        self.inner.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )

        self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        # 全局监听滚轮，但仅在鼠标位于当前滚动区域(含子控件)时才滚动
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel, add="+")
        self.canvas.bind_all("<Button-4>", self._on_mousewheel_linux, add="+")
        self.canvas.bind_all("<Button-5>", self._on_mousewheel_linux, add="+")

    def _on_mousewheel(self, event):
        if not self._pointer_in_self():
            return
        step = -1 if event.delta > 0 else 1
        self.canvas.yview_scroll(step, "units")

    def _on_mousewheel_linux(self, event):
        if not self._pointer_in_self():
            return
        if event.num == 4:
            self.canvas.yview_scroll(-1, "units")
        elif event.num == 5:
            self.canvas.yview_scroll(1, "units")

    def _pointer_in_self(self):
        w = self.winfo_containing(self.winfo_pointerx(), self.winfo_pointery())
        while w is not None:
            if w == self:
                return True
            w = w.master
        return False


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("视频重复清理 + 批量转码")

        self.ffmpeg_ok = self._check_tool("ffmpeg")
        self.ffprobe_ok = self._check_tool("ffprobe")

        self.match_folders = []
        self.match_results = []
        self.match_selection_vars = []
        self.match_skip_group_vars = []
        self.match_group_radiobuttons = []
        self.match_keep_selection_state = []
        self.match_skip_group_state = []
        self.match_page_index = 0
        self.match_file_count = 0
        self.match_analysis_running = False
        self.match_selection_enabled = False
        self.match_stop_event = threading.Event()
        self.transcode_running = False
        self.transcode_stop_event = threading.Event()

        self.log_queue = queue.Queue()

        self._build_ui()
        self._apply_windows_scaling()
        self._set_initial_window_size()
        self.after(150, self._drain_log)

    def _build_ui(self):
        self._setup_styles()
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=8, pady=8)

        self.tab_match = ttk.Frame(notebook)
        self.tab_transcode = ttk.Frame(notebook)
        notebook.add(self.tab_match, text="1) 重复视频分析")
        notebook.add(self.tab_transcode, text="2) 批量转码")

        self._build_match_tab()
        self._build_transcode_tab()

        if not (self.ffmpeg_ok and self.ffprobe_ok):
            messagebox.showwarning(
                "依赖缺失",
                "未检测到 ffmpeg 或 ffprobe。\n"
                "请先安装并加入 PATH，否则无法获取码率/分辨率或执行转码。",
            )

    # ------------------------- 公共工具 -------------------------
    def _check_tool(self, name: str) -> bool:
        try:
            subprocess.run(
                [name, "-version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            return True
        except Exception:
            return False

    def _log(self, text: str):
        self.log_queue.put(text)

    def _drain_log(self):
        while not self.log_queue.empty():
            text = self.log_queue.get_nowait()
            if text.startswith("[M]"):
                self.match_log.insert("end", text[3:] + "\n")
                self.match_log.see("end")
            elif text.startswith("[T]"):
                self.transcode_log.insert("end", text[3:] + "\n")
                self.transcode_log.see("end")
        self.after(150, self._drain_log)

    def _scan_videos(self, folders, stop_event=None):
        files = []
        for folder in folders:
            if stop_event and stop_event.is_set():
                return files
            folder_path = Path(folder)
            if not folder_path.exists():
                continue
            for root, _, names in os.walk(folder_path):
                if stop_event and stop_event.is_set():
                    return files
                for name in names:
                    if stop_event and stop_event.is_set():
                        return files
                    p = Path(root) / name
                    if p.suffix.lower() in VIDEO_EXTS:
                        try:
                            if p.stat().st_size <= 0:
                                continue
                        except OSError:
                            continue
                        files.append(p)
        return files

    def _extract_code(self, filename: str) -> str:
        text = filename.strip()
        if not text:
            return ""

        
        if "@" in text:
            text = text.rsplit("@", 1)[1]

        candidates = []

        for m in CODE_STRICT_PATTERN.finditer(text):
            prefix = m.group(1).upper().replace("-", "")
            num_raw = m.group(2)
            try:
                number = str(int(num_raw))
            except ValueError:
                number = num_raw.lstrip("0") or "0"

            if prefix in NOISE_PREFIXES:
                continue
            # 严格格式优先级更高
            candidates.append((2, m.start(), prefix, number))

        for m in CODE_LOOSE_PATTERN.finditer(text):
            prefix = m.group(1).upper()
            num_raw = m.group(2)
            try:
                number = str(int(num_raw))
            except ValueError:
                number = num_raw.lstrip("0") or "0"
            if prefix in NOISE_PREFIXES:
                continue
            candidates.append((1, m.start(), prefix, number))

        if not candidates:
            return ""

        
        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        _, _, prefix, number = candidates[0]
        return f"{prefix}{number}"

    def _get_media_meta(self, path: Path):
        if not self.ffprobe_ok:
            return "未知", "未知", "未知"

        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,bit_rate:format=duration",
            "-of",
            "json",
            str(path),
        ]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=FFPROBE_TIMEOUT_SEC,
            )
            payload = json.loads(proc.stdout)
            streams = payload.get("streams", [])
            if not streams:
                return "未知", "未知", "未知"
            s0 = streams[0]
            width = s0.get("width")
            height = s0.get("height")
            bit_rate = s0.get("bit_rate")
            duration_raw = (payload.get("format") or {}).get("duration")
            resolution = f"{width}x{height}" if width and height else "未知"
            if bit_rate:
                bitrate = f"{int(bit_rate) / 1000:.0f} kbps"
            else:
                bitrate = "未知"
            try:
                duration_val = float(duration_raw)
                duration = self._format_duration(duration_val)
            except Exception:
                duration = "未知"
            return resolution, bitrate, duration
        except Exception:
            return "未知", "未知", "未知"

    def _calc_md5(self, path: Path) -> str:
        h = hashlib.md5()
        with path.open("rb") as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                h.update(data)
        return h.hexdigest()

    # ------------------------- 功能 1：匹配并清理 -------------------------
    def _build_match_tab(self):
        self.tab_match.columnconfigure(0, weight=1)
        self.tab_match.rowconfigure(2, weight=1)

        top = ttk.LabelFrame(self.tab_match, text="目录管理")
        top.grid(row=0, column=0, sticky="ew", padx=8, pady=8)

        btn_row = ttk.Frame(top)
        btn_row.pack(fill="x", padx=8, pady=8)

        ttk.Button(btn_row, text="添加文件夹", command=self._add_match_folder).pack(side="left", padx=4)
        ttk.Button(btn_row, text="移除选中", command=self._remove_match_folder).pack(side="left", padx=4)
        self.match_start_btn = ttk.Button(btn_row, text="开始分析", command=self._start_match_analysis)
        self.match_start_btn.pack(side="left", padx=4)
        self.match_stop_btn = ttk.Button(btn_row, text="停止分析", command=self._stop_match_analysis, state="disabled")
        self.match_stop_btn.pack(side="left", padx=4)
        self.match_use_md5_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            btn_row,
            text="启用MD5精确匹配（较慢）",
            variable=self.match_use_md5_var,
        ).pack(side="left", padx=(14, 4))
        self.match_use_meta_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            btn_row,
            text="分析分辨率/码率（较慢）",
            variable=self.match_use_meta_var,
        ).pack(side="left", padx=4)

        self.match_folder_list = tk.Listbox(top, height=4, selectmode="extended")
        self.match_folder_list.pack(fill="x", padx=8, pady=(0, 8))

        progress_box = ttk.LabelFrame(self.tab_match, text="分析进度")
        progress_box.grid(row=1, column=0, sticky="ew", padx=8, pady=8)

        self.match_progress = ttk.Progressbar(progress_box, mode="determinate")
        self.match_progress.pack(fill="x", padx=8, pady=8)
        self.match_progress_label = ttk.Label(progress_box, text="等待开始")
        self.match_progress_label.pack(anchor="w", padx=8, pady=(0, 8))

        self.results_box = ttk.LabelFrame(self.tab_match, text="匹配结果（每组请选择保留文件）")
        self.results_box.grid(row=2, column=0, sticky="nsew", padx=8, pady=8)
        self.results_box.columnconfigure(0, weight=1)
        self.results_box.rowconfigure(0, weight=1)

        self.results_scroll = ScrollableFrame(self.results_box)
        self.results_scroll.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)

        bottom = ttk.Frame(self.tab_match)
        bottom.grid(row=3, column=0, sticky="ew", padx=8, pady=(0, 8))

        self.page_prev_btn = ttk.Button(bottom, text="上一页", command=lambda: self._change_match_page(-1, True), width=8)
        self.page_prev_btn.pack(side="left")
        self.page_next_btn = ttk.Button(bottom, text="下一页", command=lambda: self._change_match_page(1, True), width=8)
        self.page_next_btn.pack(side="left", padx=(6, 8))
        self.page_info_label = ttk.Label(bottom, text="")
        self.page_info_label.pack(side="left", padx=(0, 10))

        self.recommend_keep_btn = ttk.Button(
            bottom,
            text="推荐选中保留",
            command=self._recommend_keep_selection,
            state="disabled",
            width=16,
        )
        self.recommend_keep_btn.pack(side="left", padx=(0, 8))

        self.delete_unselected_btn = ttk.Button(
            bottom,
            text="确认删除未保留文件",
            command=self._start_delete_unselected,
            state="disabled",
            style="Delete.TButton",
            width=22,
        )
        self.delete_unselected_btn.pack(side="left")

        self.delete_progress = ttk.Progressbar(bottom, mode="determinate", length=300)
        self.delete_progress.pack(side="left", padx=12)
        self.delete_progress_label = ttk.Label(bottom, text="")
        self.delete_progress_label.pack(side="left")

        log_box = ttk.LabelFrame(self.tab_match, text="日志")
        log_box.grid(row=4, column=0, sticky="ew", padx=8, pady=(0, 8))
        self.match_log = tk.Text(log_box, height=4)
        self.match_log.pack(fill="both", expand=True, padx=6, pady=6)

    def _add_match_folder(self):
        folder = filedialog.askdirectory(title="选择视频文件夹")
        if not folder:
            return
        if folder not in self.match_folders:
            self.match_folders.append(folder)
            self.match_folder_list.insert("end", folder)

    def _remove_match_folder(self):
        selected = list(self.match_folder_list.curselection())
        if not selected:
            return
        selected.reverse()
        for idx in selected:
            self.match_folder_list.delete(idx)
            self.match_folders.pop(idx)

    def _start_match_analysis(self):
        if self.match_analysis_running:
            messagebox.showinfo("提示", "分析正在进行中")
            return
        if len(self.match_folders) < 1:
            messagebox.showerror("提示", "请至少添加一个文件夹进行比较")
            return

        folders = list(self.match_folders)
        use_md5 = bool(self.match_use_md5_var.get())
        use_meta = bool(self.match_use_meta_var.get())
        self.match_analysis_running = True
        self.match_selection_enabled = False
        self.match_stop_event.clear()
        self.match_results = []
        self.match_keep_selection_state = []
        self.match_skip_group_state = []
        self.match_page_index = 0
        self.match_progress["value"] = 0
        self.match_progress_label.config(text="开始扫描...")
        self.match_log.delete("1.0", "end")
        self._clear_match_cards()
        self._render_match_cards(selectable=False, show_empty=False)
        self.results_scroll.canvas.yview_moveto(0)
        self._set_match_run_buttons(running=True)
        self._set_delete_button_enabled(False)
        self._set_recommend_button_enabled(False)

        t = threading.Thread(target=self._do_match_analysis, args=(folders, use_md5, use_meta), daemon=True)
        t.start()

    def _stop_match_analysis(self):
        if not self.match_analysis_running:
            return
        self.match_stop_event.set()
        self.match_progress_label.config(text="正在停止分析...")
        self._log("[M]收到停止分析请求")
        self.match_stop_btn.config(state="disabled")

    def _set_match_run_buttons(self, running: bool):
        if running:
            self.match_start_btn.config(state="disabled")
            self.match_stop_btn.config(state="normal")
        else:
            self.match_start_btn.config(state="normal")
            self.match_stop_btn.config(state="disabled")

    def _abort_match_analysis(self):
        self.match_analysis_running = False
        self.match_selection_enabled = False
        self.after(0, self._set_match_run_buttons, False)
        self.after(0, self._set_delete_button_enabled, False)
        self.after(0, self._set_recommend_button_enabled, False)
        self.after(0, lambda: self.match_progress_label.config(text="分析已停止"))
        self._log("[M]分析已停止")

    def _do_match_analysis(self, folders, use_md5, use_meta):
        files = self._scan_videos(folders, stop_event=self.match_stop_event)
        if self.match_stop_event.is_set():
            self._abort_match_analysis()
            return
        total = len(files)
        self.match_file_count = total

        if total == 0:
            self.match_analysis_running = False
            self.after(0, self._set_match_run_buttons, False)
            self.after(0, self._set_delete_button_enabled, False)
            self.after(0, self._set_recommend_button_enabled, False)
            self.after(0, lambda: self.match_progress_label.config(text="未找到视频文件"))
            self.after(0, lambda: self._render_match_cards(selectable=False, show_empty=True))
            return

        self._log(f"[M]扫描到 {total} 个视频文件")
        self._log(f"[M]MD5精确匹配: {'开启' if use_md5 else '关闭'}")
        self._log(f"[M]分辨率/码率分析: {'开启' if use_meta else '关闭'}")

        # 阶段1：快速读取基础信息（文件名/大小），不取媒体元数据
        infos = []
        for idx, p in enumerate(files, 1):
            if self.match_stop_event.is_set():
                self._abort_match_analysis()
                return
            try:
                size = p.stat().st_size
            except OSError:
                size = 0
            code = self._extract_code(p.stem)
            infos.append(VideoInfo(path=p, size=size, resolution="未分析", bitrate="未分析", duration="未分析", code=code))

            if idx % 20 == 0 or idx == total:
                percent = idx * 35 / total
                self.after(0, self._set_match_progress, percent, f"扫描文件 {idx}/{total}")

        # 阶段2：按文件名规则匹配
        groups_by_code = {}
        for info in infos:
            if info.code:
                groups_by_code.setdefault(info.code, []).append(info)

        code_groups = []
        for code, arr in groups_by_code.items():
            if self.match_stop_event.is_set():
                self._abort_match_analysis()
                return
            if len(arr) > 1:
                code_groups.append((f"文件名规则匹配: {code}", arr))

        self.after(0, self._set_match_progress, 55, "文件名匹配完成")
        if code_groups:
            partial = self._merge_match_groups(code_groups, [])
            self.after(0, self._update_match_results_ui, partial, False)

        # 阶段3：可选 MD5 精确匹配
        md5_groups = []
        if use_md5:
            by_size = {}
            for info in infos:
                by_size.setdefault(info.size, []).append(info)

            md5_candidates = []
            for size, arr in by_size.items():
                if size > 0 and len(arr) > 1:
                    md5_candidates.extend(arr)

            self._log(f"[M]MD5候选文件数: {len(md5_candidates)}")

            md5_map = {}
            md5_total = len(md5_candidates)
            if md5_total == 0:
                self.after(0, self._set_match_progress, 85, "无同大小候选，跳过MD5")
            else:
                for idx, info in enumerate(md5_candidates, 1):
                    if self.match_stop_event.is_set():
                        self._abort_match_analysis()
                        return
                    try:
                        info.md5 = self._calc_md5(info.path)
                        md5_map.setdefault(info.md5, []).append(info)
                    except Exception as e:
                        self._log(f"[M]MD5失败: {info.path} ({e})")

                    if idx % 2 == 0 or idx == md5_total:
                        percent = 55 + idx * 30 / md5_total
                        self.after(0, self._set_match_progress, percent, f"计算MD5 {idx}/{md5_total}")
                    if idx % 10 == 0 or idx == md5_total:
                        md5_partial = []
                        for md5, arr in md5_map.items():
                            if len(arr) > 1:
                                md5_partial.append((f"MD5匹配: {md5[:12]}...", arr))
                        partial = self._merge_match_groups(code_groups, md5_partial)
                        self.after(0, self._update_match_results_ui, partial, False)

            for md5, arr in md5_map.items():
                if len(arr) > 1:
                    md5_groups.append((f"MD5匹配: {md5[:12]}...", arr))
        else:
            self.after(0, self._set_match_progress, 85, "已跳过MD5")

        merged = self._merge_match_groups(code_groups, md5_groups)

        # 阶段4：可选补充媒体元数据（分辨率/码率）
        if use_meta:
            if not self._fill_media_meta_for_results(merged, progress_start=85, progress_end=99):
                return
        else:
            self.after(0, self._set_match_progress, 99, "已跳过分辨率/码率分析")

        self.match_results = merged
        self.match_analysis_running = False
        self.match_selection_enabled = True
        self.after(0, self._set_match_run_buttons, False)
        self.after(0, self._set_delete_button_enabled, bool(merged))

        self.after(0, self._set_match_progress, 100, "分析完成")
        self.after(0, self._update_match_results_ui, merged, True)
        self.after(
            0,
            lambda: self.match_progress_label.config(
                text=f"分析完成：{total} 个文件，匹配组 {len(merged)}"
            ),
        )

        self._log(f"[M]文件名匹配组: {len(code_groups)}")
        self._log(f"[M]MD5匹配组: {len(md5_groups)}")
        self._log(f"[M]去重后匹配组: {len(merged)}")

    def _fill_media_meta_for_results(self, groups, progress_start=85, progress_end=99):
        unique_infos = {}
        for _, arr in groups:
            for info in arr:
                unique_infos[str(info.path).lower()] = info

        items = list(unique_infos.values())
        total = len(items)
        if total == 0:
            return True

        for idx, info in enumerate(items, 1):
            if self.match_stop_event.is_set():
                self._abort_match_analysis()
                return False
            resolution, bitrate, duration = self._get_media_meta(info.path)
            info.resolution = resolution
            info.bitrate = bitrate
            info.duration = duration

            if idx % 5 == 0 or idx == total:
                percent = progress_start + idx * (progress_end - progress_start) / total
                self.after(0, self._set_match_progress, percent, f"补充媒体信息 {idx}/{total}")
        return True

    def _format_size(self, size):
        if size <= 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        s = float(size)
        while s >= 1024 and i < len(units) - 1:
            s /= 1024
            i += 1
        return f"{s:.2f} {units[i]}"

    def _format_duration(self, seconds):
        if seconds is None or seconds < 0:
            return "未知"
        total = int(round(seconds))
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        if h > 0:
            return f"{h:02d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"

    def _set_match_progress(self, val, text):
        self.match_progress["value"] = val
        self.match_progress_label.config(text=text)

    def _parse_resolution_pixels(self, resolution: str) -> int:
        if not resolution:
            return 0
        m = re.match(r"^\s*(\d+)\s*x\s*(\d+)\s*$", resolution, re.IGNORECASE)
        if not m:
            return 0
        try:
            w = int(m.group(1))
            h = int(m.group(2))
            return max(0, w * h)
        except Exception:
            return 0

    def _parse_resolution_dims(self, resolution: str):
        if not resolution:
            return None
        m = re.match(r"^\s*(\d+)\s*x\s*(\d+)\s*$", resolution, re.IGNORECASE)
        if not m:
            return None
        try:
            return int(m.group(1)), int(m.group(2))
        except Exception:
            return None

    def _parse_bitrate_kbps(self, bitrate: str) -> int:
        if not bitrate:
            return 0
        m = re.search(r"(\d+)", bitrate)
        if not m:
            return 0
        try:
            return int(m.group(1))
        except Exception:
            return 0

    def _parse_duration_seconds(self, duration: str) -> int:
        if not duration:
            return 0
        m = re.match(r"^\s*(?:(\d+):)?(\d{1,2}):(\d{1,2})\s*$", duration)
        if not m:
            return 0
        try:
            h = int(m.group(1) or 0)
            mm = int(m.group(2))
            ss = int(m.group(3))
            return h * 3600 + mm * 60 + ss
        except Exception:
            return 0

    def _format_priority(self, path: Path) -> int:
        ext = path.suffix.lower()
        if ext == ".mp4":
            return 4
        if ext == ".mkv":
            return 3
        if ext == ".avi":
            return 2
        return 1

    def _recommend_keep_selection(self):
        if self.match_analysis_running or not self.match_selection_enabled:
            messagebox.showinfo("提示", "分析未完成，暂不可推荐选中")
            return
        if not self.match_results:
            messagebox.showinfo("提示", "没有可推荐的匹配结果")
            return

        start_idx = self.match_page_index * MATCH_PAGE_SIZE
        end_idx = min(len(self.match_results), start_idx + MATCH_PAGE_SIZE)
        changed = 0
        for gidx in range(start_idx, end_idx):
            _, infos = self.match_results[gidx]
            if not infos:
                continue
            if gidx < len(self.match_skip_group_state) and self.match_skip_group_state[gidx]:
                continue

            # 元数据不可读（分辨率/码率/时长）默认不自动选中
            candidates = []
            for i, info in enumerate(infos):
                dims = self._parse_resolution_dims(info.resolution)
                res = (dims[0] * dims[1]) if dims else 0
                br = self._parse_bitrate_kbps(info.bitrate)
                dur = self._parse_duration_seconds(info.duration)
                if res <= 0 or br <= 0 or dur <= 0:
                    continue
                short_edge = min(dims[0], dims[1]) if dims else 0
                is_hd_720 = short_edge >= 720
                candidates.append((i, info, res, br, is_hd_720))

            if not candidates:
                if gidx < len(self.match_keep_selection_state):
                    self.match_keep_selection_state[gidx] = -1
                continue

            # 先按 720p 分层：有 >=720p 的话只在该层里选；否则在 <720p 里选
            hd_candidates = [t for t in candidates if t[4]]
            pool = hd_candidates if hd_candidates else candidates

            # 优先级：格式 > 容量(小) > 分辨率(高) > 码率(高)
            best_i, _, _, _, _ = min(
                pool,
                key=lambda t: (
                    -self._format_priority(t[1].path),
                    t[1].size,
                    -t[2],
                    -t[3],
                    t[1].path.name.lower(),
                ),
            )

            if gidx < len(self.match_keep_selection_state) and self.match_keep_selection_state[gidx] != best_i:
                self.match_keep_selection_state[gidx] = best_i
                changed += 1

        # 刷新当前页显示
        self._render_match_cards(selectable=True, show_empty=True)
        self._log(f"[M]推荐选中完成（当前页），已更新 {changed} 组")

    def _clear_match_cards(self):
        for w in self.results_scroll.inner.winfo_children():
            w.destroy()
        self.match_selection_vars = []
        self.match_skip_group_vars = []
        self.match_group_radiobuttons = []

    def _merge_match_groups(self, code_groups, md5_groups):
        merged = []
        seen = set()

        def sig(arr):
            return tuple(sorted(str(x.path).lower() for x in arr))

        for title, arr in code_groups + md5_groups:
            s = sig(arr)
            if s in seen:
                continue
            seen.add(s)
            merged.append((title, arr))
        return merged

    def _update_match_results_ui(self, results, selectable):
        self.match_results = results
        self.match_selection_enabled = selectable
        # 结果刷新时重置跨页状态，避免与旧结果错位
        self.match_page_index = 0
        self.match_keep_selection_state = [-1] * len(results)
        self.match_skip_group_state = [False] * len(results)
        ui_enabled = selectable and bool(results)
        self._set_delete_button_enabled(ui_enabled)
        self._set_recommend_button_enabled(ui_enabled)
        self._render_match_cards(selectable=selectable, show_empty=True)
        self.results_scroll.canvas.yview_moveto(0)

    def _set_delete_button_enabled(self, enabled: bool):
        self.delete_unselected_btn.config(state="normal" if enabled else "disabled")

    def _set_recommend_button_enabled(self, enabled: bool):
        self.recommend_keep_btn.config(state="normal" if enabled else "disabled")

    def _change_match_page(self, delta: int, selectable: bool):
        total_groups = len(self.match_results)
        total_pages = max(1, (total_groups + MATCH_PAGE_SIZE - 1) // MATCH_PAGE_SIZE)
        new_page = max(0, min(total_pages - 1, self.match_page_index + delta))
        if new_page == self.match_page_index:
            return
        self.match_page_index = new_page
        self._render_match_cards(selectable=selectable, show_empty=True)
        self.results_scroll.canvas.yview_moveto(0)

    def _update_page_controls(self):
        total_groups = len(self.match_results)
        if total_groups <= 0:
            self.page_prev_btn.config(state="disabled")
            self.page_next_btn.config(state="disabled")
            self.page_info_label.config(text="")
            return

        total_pages = max(1, (total_groups + MATCH_PAGE_SIZE - 1) // MATCH_PAGE_SIZE)
        self.match_page_index = max(0, min(self.match_page_index, total_pages - 1))
        start_idx = self.match_page_index * MATCH_PAGE_SIZE
        end_idx = min(total_groups, start_idx + MATCH_PAGE_SIZE)

        self.page_prev_btn.config(state="normal" if self.match_page_index > 0 else "disabled")
        self.page_next_btn.config(state="normal" if self.match_page_index < total_pages - 1 else "disabled")
        self.page_info_label.config(text=f"第 {self.match_page_index + 1}/{total_pages} 页（组 {start_idx + 1}-{end_idx} / {total_groups}）")

    def _on_keep_var_changed(self, gidx: int, keep_var: tk.IntVar):
        if 0 <= gidx < len(self.match_keep_selection_state):
            self.match_keep_selection_state[gidx] = keep_var.get()

    def _toggle_skip_group_page(
        self,
        gidx: int,
        skip_var: tk.BooleanVar,
        keep_var: tk.IntVar,
        rb_refs,
        selectable: bool,
    ):
        skip = bool(skip_var.get())
        if 0 <= gidx < len(self.match_skip_group_state):
            self.match_skip_group_state[gidx] = skip
        if skip and 0 <= gidx < len(self.match_keep_selection_state):
            self.match_keep_selection_state[gidx] = -1
        keep_var.set(-1 if skip else self.match_keep_selection_state[gidx])
        for rb in rb_refs:
            if skip or not selectable:
                rb.state(["disabled"])
            else:
                rb.state(["!disabled"])

    def _render_match_cards(self, selectable=True, show_empty=True):
        self._clear_match_cards()
        self._update_page_controls()

        if not self.match_results:
            if show_empty:
                empty_text = "没有发现重复匹配项"
                if self.match_analysis_running:
                    empty_text = "分析中，匹配结果将实时显示在这里..."
                ttk.Label(self.results_scroll.inner, text=empty_text).pack(anchor="w", padx=8, pady=8)
            return

        total_groups = len(self.match_results)
        total_pages = max(1, (total_groups + MATCH_PAGE_SIZE - 1) // MATCH_PAGE_SIZE)
        if self.match_page_index >= total_pages:
            self.match_page_index = total_pages - 1
        if self.match_page_index < 0:
            self.match_page_index = 0

        start_idx = self.match_page_index * MATCH_PAGE_SIZE
        end_idx = min(total_groups, start_idx + MATCH_PAGE_SIZE)

        page_groups = self.match_results[start_idx:end_idx]
        for offset, (title, infos) in enumerate(page_groups):
            gidx = start_idx + offset
            frame = ttk.LabelFrame(self.results_scroll.inner, text=f"匹配组 {gidx + 1} - {title}")
            frame.pack(fill="x", padx=8, pady=6)

            keep_init = -1
            if gidx < len(self.match_keep_selection_state):
                keep_init = self.match_keep_selection_state[gidx]
            keep_var = tk.IntVar(value=keep_init)
            self.match_selection_vars.append(keep_var)
            skip_init = False
            if gidx < len(self.match_skip_group_state):
                skip_init = self.match_skip_group_state[gidx]
            skip_var = tk.BooleanVar(value=skip_init)
            self.match_skip_group_vars.append(skip_var)
            self.match_group_radiobuttons.append([])
            rb_refs = self.match_group_radiobuttons[-1]

            top_actions = ttk.Frame(frame)
            top_actions.pack(fill="x", padx=6, pady=(4, 0))
            skip_cb = ttk.Checkbutton(
                top_actions,
                text="不去除重复视频",
                variable=skip_var,
                command=lambda idx=gidx, sv=skip_var, kv=keep_var, rbs=rb_refs: self._toggle_skip_group_page(
                    idx, sv, kv, rbs, selectable
                ),
            )
            if not selectable:
                skip_cb.state(["disabled"])
            skip_cb.pack(side="left")

            keep_var.trace_add("write", lambda *_args, idx=gidx, v=keep_var: self._on_keep_var_changed(idx, v))

            for i, info in enumerate(infos):
                item = ttk.Frame(frame)
                item.pack(fill="x", padx=6, pady=4)

                rb = ttk.Radiobutton(item, text="保留此文件", variable=keep_var, value=i)
                if not selectable:
                    rb.state(["disabled"])
                rb_refs.append(rb)
                rb.grid(row=0, column=0, rowspan=3, padx=(0, 10), sticky="n")

                ttk.Label(item, text=f"文件名: {info.path.name}").grid(row=0, column=1, sticky="w")
                ttk.Label(item, text=f"大小: {self._format_size(info.size)}").grid(row=0, column=2, padx=12, sticky="w")
                ttk.Label(item, text=f"分辨率: {info.resolution}").grid(row=1, column=1, sticky="w")
                ttk.Label(item, text=f"码率: {info.bitrate}").grid(row=1, column=2, padx=12, sticky="w")
                ttk.Label(item, text=f"时长: {info.duration}").grid(row=1, column=3, padx=12, sticky="w")
                ttk.Label(item, text=f"位置: {info.path}").grid(row=2, column=1, columnspan=2, sticky="w")

            ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=(4, 0))

            # 首次渲染时根据跳过状态应用禁用
            if skip_var.get():
                for rb in rb_refs:
                    rb.state(["disabled"])

    def _start_delete_unselected(self):
        if self.match_analysis_running or not self.match_selection_enabled:
            messagebox.showinfo("提示", "分析未完成，暂不可选择保留文件或执行删除")
            return

        if not self.match_results:
            messagebox.showinfo("提示", "没有可删除的匹配项")
            return

        delete_items = []
        keep_to_rename = []
        processed_group_indices = []
        for gidx, (_, infos) in enumerate(self.match_results):
            if gidx < len(self.match_skip_group_state) and self.match_skip_group_state[gidx]:
                # 勾选“不去除重复视频”的组跳过删除
                continue
            keep_idx = self.match_keep_selection_state[gidx] if gidx < len(self.match_keep_selection_state) else -1
            if keep_idx < 0 or keep_idx >= len(infos):
                # 未选择保留文件的组不做任何操作
                continue
            keep_path = infos[keep_idx].path
            keep_to_rename.append(keep_path)
            has_delete = False
            for i, info in enumerate(infos):
                if i != keep_idx:
                    delete_items.append((info.path, keep_path))
                    has_delete = True
            if has_delete:
                processed_group_indices.append(gidx)

        if not delete_items:
            messagebox.showinfo("提示", "未选择任何保留文件，未执行任何操作")
            return

        ok = self._confirm_delete_with_preview(delete_items)
        if not ok:
            return

        self.delete_progress["value"] = 0
        self.delete_progress_label.config(text="开始删除...")
        self._set_delete_button_enabled(False)
        self._set_recommend_button_enabled(False)

        to_delete = [p for p, _ in delete_items]
        t = threading.Thread(
            target=self._do_delete_files,
            args=(to_delete, keep_to_rename, processed_group_indices),
            daemon=True,
        )
        t.start()

    def _confirm_delete_with_preview(self, delete_items):
        dlg = tk.Toplevel(self)
        dlg.title("高风险操作确认")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(True, True)

        self.update_idletasks()
        w = min(1100, max(760, int(self.winfo_width() * 0.82)))
        h = min(720, max(420, int(self.winfo_height() * 0.75)))
        x = self.winfo_rootx() + max(20, (self.winfo_width() - w) // 2)
        y = self.winfo_rooty() + max(20, (self.winfo_height() - h) // 2)
        dlg.geometry(f"{w}x{h}+{x}+{y}")

        top = ttk.Frame(dlg, padding=10)
        top.pack(fill="both", expand=True)

        warn = (
            "该操作不可恢复，请用户谨慎删除，建议先检查完所有重复组再点击，"
            "否则会造成不可逆的后果！"
        )
        ttk.Label(top, text=warn, foreground="#B00020", wraplength=w - 60, justify="left").pack(
            anchor="w", pady=(0, 8)
        )
        ttk.Label(top, text=f"本次将删除 {len(delete_items)} 个文件：", justify="left").pack(anchor="w", pady=(0, 6))

        box = ttk.Frame(top)
        box.pack(fill="both", expand=True)

        preview = tk.Text(box, height=20, wrap="none")
        yscroll = ttk.Scrollbar(box, orient="vertical", command=preview.yview)
        xscroll = ttk.Scrollbar(box, orient="horizontal", command=preview.xview)
        preview.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        preview.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        box.rowconfigure(0, weight=1)
        box.columnconfigure(0, weight=1)

        for del_path, keep_path in delete_items:
            preview.insert("end", f"{del_path}    原因：与{keep_path.name}重复\n")
        preview.config(state="disabled")

        btns = ttk.Frame(top)
        btns.pack(fill="x", pady=(10, 0))

        result = {"ok": False}

        def on_confirm():
            result["ok"] = True
            dlg.destroy()

        def on_cancel():
            dlg.destroy()

        cancel_btn = ttk.Button(btns, text="取消", command=on_cancel, width=10)
        cancel_btn.pack(side="right")
        ttk.Button(btns, text="确定删除", command=on_confirm, width=12).pack(side="right", padx=(0, 8))

        dlg.protocol("WM_DELETE_WINDOW", on_cancel)
        dlg.bind("<Escape>", lambda _e: on_cancel())
        dlg.bind("<Return>", lambda _e: on_cancel())  # 默认回车=取消，降低误删风险
        cancel_btn.focus_set()

        self.wait_window(dlg)
        return result["ok"]

    def _normalize_keep_name(self, path: Path):
        stem = path.stem.strip()
        if "@" in stem:
            stem = stem.rsplit("@", 1)[1].strip()
        s = stem.upper().replace("_", "-").replace(" ", "-")
        s = re.sub(r"-{2,}", "-", s).strip("-")
        if not s:
            return None

        
        m_fc2 = re.match(r"^FC2[- ]*([A-Z0-9]{2,12})[- ]*0*(\d{2,8})(?:\D.*)?$", s, re.IGNORECASE)
        if m_fc2:
            prefix = m_fc2.group(1).upper()
            number = str(int(m_fc2.group(2)))
            return f"FC2-{prefix}-{number}"

        
        m_3 = re.match(r"^(\d{3})([A-Z]{2,8})[- ]*0*(\d{1,3})(?:\D.*)?$", s, re.IGNORECASE)
        if m_3:
            lead = m_3.group(1)
            alpha = m_3.group(2).upper()
            num = int(m_3.group(3))
            return f"{lead}{alpha}-{num:03d}"

        
        m_a = re.match(r"^([A-Z]{2,8})[- ]*0*(\d{1,4})(?:\D.*)?$", s, re.IGNORECASE)
        if m_a:
            alpha = m_a.group(1).upper()
            num = int(m_a.group(2))
            return f"{alpha}-{num:03d}"

        return None

    def _rename_kept_files(self, keep_paths):
        renamed = 0
        failed = 0
        skipped = 0
        seen = set()

        for p in keep_paths:
            key = str(p).lower()
            if key in seen:
                continue
            seen.add(key)
            if not p.exists():
                failed += 1
                self._log(f"[M]重命名失败(文件不存在): {p}")
                continue

            new_stem = self._normalize_keep_name(p)
            if not new_stem:
                skipped += 1
                continue

            source_ext = p.suffix
            target = p.with_name(new_stem + source_ext)
            if target == p:
                skipped += 1
                continue
            if target.exists():
                failed += 1
                self._log(f"[M]重命名失败(目标已存在): {target}")
                continue

            try:
                p.rename(target)
                renamed += 1
                self._log(f"[M]重命名: {p.name} -> {target.name}")
            except Exception as e:
                failed += 1
                self._log(f"[M]重命名失败: {p} ({e})")

        return renamed, failed, skipped

    def _do_delete_files(self, paths, keep_paths, processed_group_indices):
        total = len(paths)
        deleted = 0
        failed = 0

        for idx, p in enumerate(paths, 1):
            try:
                if p.exists():
                    p.unlink()
                    deleted += 1
                    self._log(f"[M]删除: {p}")
            except Exception as e:
                failed += 1
                self._log(f"[M]删除失败: {p} ({e})")

            percent = idx * 100 / total
            self.after(0, self._set_delete_progress, percent, f"删除中 {idx}/{total}")

        self.after(0, self._set_delete_progress, 100, "删除完成，正在重命名保留文件...")
        renamed, rename_failed, rename_skipped = self._rename_kept_files(keep_paths)

        self.after(
            0,
            lambda: self.delete_progress_label.config(
                text=f"删除完成，成功 {deleted}，失败 {failed}；重命名 成功 {renamed}，失败 {rename_failed}，跳过 {rename_skipped}"
            ),
        )
        self.after(0, self._remove_processed_groups_after_delete, processed_group_indices)

    def _set_delete_progress(self, val, text):
        self.delete_progress["value"] = val
        self.delete_progress_label.config(text=text)

    def _reset_match_after_delete(self):
        # 删除后回到未分析初始状态（日志保留）
        self.match_results = []
        self.match_selection_vars = []
        self.match_keep_selection_state = []
        self.match_skip_group_state = []
        self.match_page_index = 0
        self.match_selection_enabled = False
        self.match_analysis_running = False
        self.match_progress["value"] = 0
        self.match_progress_label.config(text="等待开始")
        self._set_match_run_buttons(False)
        self._set_delete_button_enabled(False)
        self._set_recommend_button_enabled(False)
        self._clear_match_cards()

    def _remove_processed_groups_after_delete(self, processed_group_indices):
        if not processed_group_indices:
            ui_enabled = self.match_selection_enabled and bool(self.match_results)
            self._set_delete_button_enabled(ui_enabled)
            self._set_recommend_button_enabled(ui_enabled)
            return

        for idx in sorted(set(processed_group_indices), reverse=True):
            if 0 <= idx < len(self.match_results):
                self.match_results.pop(idx)
            if 0 <= idx < len(self.match_keep_selection_state):
                self.match_keep_selection_state.pop(idx)
            if 0 <= idx < len(self.match_skip_group_state):
                self.match_skip_group_state.pop(idx)

        total_groups = len(self.match_results)
        total_pages = max(1, (total_groups + MATCH_PAGE_SIZE - 1) // MATCH_PAGE_SIZE)
        if self.match_page_index >= total_pages:
            self.match_page_index = total_pages - 1
        if self.match_page_index < 0:
            self.match_page_index = 0

        self.match_selection_enabled = True
        ui_enabled = bool(self.match_results)
        self._set_delete_button_enabled(ui_enabled)
        self._set_recommend_button_enabled(ui_enabled)
        self._render_match_cards(selectable=True, show_empty=True)

    # ------------------------- 功能 2：批量转码 -------------------------
    def _build_transcode_tab(self):
        box = ttk.LabelFrame(self.tab_transcode, text="转码参数")
        box.pack(fill="x", padx=8, pady=8)

        ttk.Label(box, text="源目录").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.src_dir_var = tk.StringVar()
        ttk.Entry(box, textvariable=self.src_dir_var, width=80).grid(row=0, column=1, sticky="we", padx=6, pady=6)
        ttk.Button(box, text="选择", command=self._pick_src_dir).grid(row=0, column=2, padx=6, pady=6)

        ttk.Label(box, text="临时目录").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        self.temp_dir_var = tk.StringVar(value=r"F:\temp")
        ttk.Entry(box, textvariable=self.temp_dir_var, width=80).grid(row=1, column=1, sticky="we", padx=6, pady=6)
        ttk.Button(box, text="选择", command=self._pick_temp_dir).grid(row=1, column=2, padx=6, pady=6)

        ttk.Label(box, text="分辨率").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        self.resolution_var = tk.StringVar(value="保持原分辨率")
        self.resolution_combo = ttk.Combobox(
            box,
            textvariable=self.resolution_var,
            values=["保持原分辨率", "1920x1080", "1280x720", "854x480"],
            state="readonly",
            width=20,
        )
        self.resolution_combo.grid(row=2, column=1, sticky="w", padx=6, pady=6)
        self.resolution_combo.bind("<<ComboboxSelected>>", self._on_transcode_resolution_changed)

        ttk.Label(box, text="编码器").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        self.codec_var = tk.StringVar(value="libx265")
        ttk.Combobox(
            box,
            textvariable=self.codec_var,
            values=["libx265", "libx264", "libsvtav1", "h264_nvenc", "hevc_nvenc", "av1_nvenc", "h264_amf", "hevc_amf", "av1_amf"],
            state="readonly",
            width=20,
        ).grid(row=3, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(box, text="编码模式").grid(row=4, column=0, sticky="w", padx=6, pady=6)
        self.rate_mode_var = tk.StringVar(value="CRF")
        ttk.Combobox(
            box,
            textvariable=self.rate_mode_var,
            values=["CRF", "CBR"],
            state="readonly",
            width=20,
        ).grid(row=4, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(box, text="CRF值(模式=CRF)").grid(row=5, column=0, sticky="w", padx=6, pady=6)
        self.crf_var = tk.StringVar(value="24")
        ttk.Entry(box, textvariable=self.crf_var, width=20).grid(row=5, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(box, text="码率(模式=CBR, 如 2500k)").grid(row=6, column=0, sticky="w", padx=6, pady=6)
        self.bitrate_var = tk.StringVar(value="2500k")
        ttk.Entry(box, textvariable=self.bitrate_var, width=20).grid(row=6, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(box, text="小于此大小不转码").grid(row=7, column=0, sticky="w", padx=6, pady=6)
        self.min_size_gb_var = tk.StringVar(value="2")
        ttk.Combobox(
            box,
            textvariable=self.min_size_gb_var,
            values=["1", "2", "3", "4", "5"],
            state="readonly",
            width=20,
        ).grid(row=7, column=1, sticky="w", padx=6, pady=6)

        action_row = ttk.Frame(box)
        action_row.grid(row=8, column=1, sticky="w", padx=6, pady=10)
        self.transcode_start_btn = ttk.Button(action_row, text="开始批量转码", command=self._start_transcode)
        self.transcode_start_btn.pack(side="left")
        self.transcode_stop_btn = ttk.Button(action_row, text="停止转码", command=self._request_stop_transcode, state="disabled")
        self.transcode_stop_btn.pack(side="left", padx=(8, 0))

        box.columnconfigure(1, weight=1)

        pbox = ttk.LabelFrame(self.tab_transcode, text="转码进度")
        pbox.pack(fill="x", padx=8, pady=8)

        self.transcode_progress = ttk.Progressbar(pbox, mode="determinate")
        self.transcode_progress.pack(fill="x", padx=8, pady=8)
        self.transcode_progress_label = ttk.Label(pbox, text="等待开始")
        self.transcode_progress_label.pack(anchor="w", padx=8, pady=(0, 8))

        self.transcode_file_progress = ttk.Progressbar(pbox, mode="determinate")
        self.transcode_file_progress.pack(fill="x", padx=8, pady=(0, 8))
        self.transcode_file_progress_label = ttk.Label(pbox, text="当前文件：等待开始")
        self.transcode_file_progress_label.pack(anchor="w", padx=8, pady=(0, 8))

        log_box = ttk.LabelFrame(self.tab_transcode, text="日志")
        log_box.pack(fill="both", expand=True, padx=8, pady=8)
        self.transcode_log = tk.Text(log_box)
        self.transcode_log.pack(fill="both", expand=True, padx=6, pady=6)

        self._apply_transcode_recommendation(initial=True)

    def _pick_src_dir(self):
        d = filedialog.askdirectory(title="选择待转码目录")
        if d:
            self.src_dir_var.set(d)

    def _pick_temp_dir(self):
        d = filedialog.askdirectory(title="选择临时目录")
        if d:
            self.temp_dir_var.set(d)

    def _on_transcode_resolution_changed(self, _event=None):
        self._apply_transcode_recommendation(initial=False)

    def _apply_transcode_recommendation(self, initial=False):
        resolution = self.resolution_var.get()
        presets = {
            "1920x1080": {"codec": "av1_amf", "crf": "25", "bitrate": "1650k"},
            "1280x720": {"codec": "av1_amf", "crf": "25", "bitrate": "1050k"},
        }
        p = presets.get(resolution)
        if not p:
            return

        self.codec_var.set(p["codec"])
        self.crf_var.set(p["crf"])
        self.bitrate_var.set(p["bitrate"])
        self.rate_mode_var.set("CBR")

        if not initial:
            self._log(
                f"[T]已按分辨率推荐参数: {resolution} -> 编码器 {p['codec']}, CRF {p['crf']}, 码率 {p['bitrate']}"
            )

    def _start_transcode(self):
        if self.transcode_running:
            messagebox.showinfo("提示", "转码任务正在进行中")
            return

        src_dir = Path(self.src_dir_var.get().strip())
        temp_dir = Path(self.temp_dir_var.get().strip())

        if not src_dir.exists() or not src_dir.is_dir():
            messagebox.showerror("错误", "源目录不存在")
            return

        if not self.ffmpeg_ok or not self.ffprobe_ok:
            messagebox.showerror("错误", "请先安装 ffmpeg / ffprobe 并加入 PATH")
            return

        params = {
            "resolution": self.resolution_var.get(),
            "codec": self.codec_var.get(),
            "rate_mode": self.rate_mode_var.get(),
            "crf": self.crf_var.get(),
            "bitrate": self.bitrate_var.get(),
            "min_size_gb": int(self.min_size_gb_var.get()),
        }

        self.transcode_log.delete("1.0", "end")
        self.transcode_progress["value"] = 0
        self.transcode_file_progress["value"] = 0
        self.transcode_file_progress_label.config(text="当前文件：等待开始")
        self.transcode_stop_event.clear()
        self.transcode_running = True
        self.transcode_start_btn.config(state="disabled")
        self.transcode_stop_btn.config(state="normal")

        t = threading.Thread(
            target=self._do_transcode,
            args=(src_dir, temp_dir, params),
            daemon=True,
        )
        t.start()

    def _request_stop_transcode(self):
        if not self.transcode_running:
            return
        self.transcode_stop_event.set()
        self.transcode_stop_btn.config(state="disabled")
        self.transcode_progress_label.config(text="已请求停止，当前文件完成后将停止...")
        self._log("[T]收到停止请求：当前文件完成后停止")

    def _set_transcode_run_buttons(self, running: bool):
        self.transcode_running = running
        self.transcode_start_btn.config(state="disabled" if running else "normal")
        self.transcode_stop_btn.config(state="normal" if running else "disabled")

    def _safe_marker_component(self, text: str) -> str:
        return re.sub(r'[\\\\/:*?\"<>|]', "_", (text or "").strip())

    def _resume_marker_path(self, temp_dir: Path, src_dir: Path, next_name: str) -> Path:
        src_tag = self._safe_marker_component(src_dir.name)
        file_tag = self._safe_marker_component(next_name)
        return temp_dir / f"{src_tag}_{file_tag}"

    def _clear_resume_markers(self, temp_dir: Path, src_dir: Path):
        prefix = self._safe_marker_component(src_dir.name) + "_"
        for p in temp_dir.glob(f"{prefix}*"):
            try:
                if p.is_file():
                    p.unlink()
            except Exception:
                pass

    def _find_resume_index(self, files, temp_dir: Path, src_dir: Path):
        prefix = self._safe_marker_component(src_dir.name) + "_"
        marker = None
        for p in temp_dir.glob(f"{prefix}*"):
            if p.is_file():
                marker = p
                break
        if not marker:
            return 0
        next_name = marker.name[len(prefix):]
        for idx, f in enumerate(files):
            if self._safe_marker_component(f.name) == next_name:
                self._log(f"[T]发现断点标记，继续从: {f.name}")
                return idx
        return 0

    def _get_video_numeric_meta(self, path: Path):
        if not self.ffprobe_ok:
            return None
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,bit_rate:format=duration",
            "-of",
            "json",
            str(path),
        ]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=FFPROBE_TIMEOUT_SEC)
            payload = json.loads(proc.stdout or "{}")
            streams = payload.get("streams") or []
            if not streams:
                return None
            s0 = streams[0]
            width = int(s0.get("width") or 0)
            height = int(s0.get("height") or 0)
            br = int(s0.get("bit_rate") or 0)
            duration = float((payload.get("format") or {}).get("duration") or 0)
            return {"width": width, "height": height, "bitrate": br, "duration": duration}
        except Exception:
            return None

    def _target_resolution(self, text: str):
        m = re.match(r"^\s*(\d+)\s*x\s*(\d+)\s*$", text or "", re.IGNORECASE)
        if not m:
            return None
        return int(m.group(1)), int(m.group(2))

    def _target_bitrate_bps(self, params):
        if params.get("rate_mode") != "CBR":
            return None
        br = (params.get("bitrate") or "").strip().lower()
        m = re.match(r"^(\d+(?:\.\d+)?)\s*([kmg])?$", br)
        if not m:
            return None
        val = float(m.group(1))
        unit = m.group(2) or ""
        if unit == "k":
            return int(val * 1000)
        if unit == "m":
            return int(val * 1000 * 1000)
        if unit == "g":
            return int(val * 1000 * 1000 * 1000)
        return int(val)

    def _safe_temp_name(self, src_dir: Path, rel: Path) -> str:
        src_tag = self._safe_marker_component(src_dir.name)
        rel_tag = self._safe_marker_component(str(rel).replace("\\", "__").replace("/", "__"))
        return f"{src_tag}__{rel_tag}"

    def _read_file_times(self, path: Path):
        st = path.stat()
        times = {
            "atime_ns": st.st_atime_ns,
            "mtime_ns": st.st_mtime_ns,
            "ctime_ns": getattr(st, "st_ctime_ns", st.st_mtime_ns),
        }
        if os.name != "nt":
            return times

        try:
            FILE_READ_ATTRIBUTES = 0x0080
            OPEN_EXISTING = 3
            FILE_SHARE_READ = 0x00000001
            FILE_SHARE_WRITE = 0x00000002
            FILE_SHARE_DELETE = 0x00000004
            INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

            class FILETIME(ctypes.Structure):
                _fields_ = [("dwLowDateTime", ctypes.wintypes.DWORD), ("dwHighDateTime", ctypes.wintypes.DWORD)]

            def filetime_to_ns(ft):
                v = (int(ft.dwHighDateTime) << 32) | int(ft.dwLowDateTime)
                return max(0, (v - 116444736000000000) * 100)

            handle = ctypes.windll.kernel32.CreateFileW(
                str(path),
                FILE_READ_ATTRIBUTES,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                None,
                OPEN_EXISTING,
                0,
                None,
            )
            if handle != INVALID_HANDLE_VALUE:
                try:
                    c = FILETIME()
                    a = FILETIME()
                    m = FILETIME()
                    ok = ctypes.windll.kernel32.GetFileTime(handle, ctypes.byref(c), ctypes.byref(a), ctypes.byref(m))
                    if ok:
                        times["ctime_ns"] = filetime_to_ns(c)
                        times["atime_ns"] = filetime_to_ns(a)
                        times["mtime_ns"] = filetime_to_ns(m)
                finally:
                    ctypes.windll.kernel32.CloseHandle(handle)
        except Exception:
            pass
        return times

    def _set_file_times(self, target: Path, times):
        try:
            os.utime(target, ns=(times["atime_ns"], times["mtime_ns"]))
        except Exception:
            pass

        if os.name != "nt":
            return
        try:
            FILE_WRITE_ATTRIBUTES = 0x0100
            OPEN_EXISTING = 3
            FILE_SHARE_READ = 0x00000001
            FILE_SHARE_WRITE = 0x00000002
            FILE_SHARE_DELETE = 0x00000004
            INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

            class FILETIME(ctypes.Structure):
                _fields_ = [("dwLowDateTime", ctypes.wintypes.DWORD), ("dwHighDateTime", ctypes.wintypes.DWORD)]

            def ns_to_filetime(ns):
                # Unix epoch(ns) -> Windows FILETIME(100ns since 1601-01-01)
                ft = int(ns / 100) + 116444736000000000
                return FILETIME(ft & 0xFFFFFFFF, (ft >> 32) & 0xFFFFFFFF)

            handle = ctypes.windll.kernel32.CreateFileW(
                str(target),
                FILE_WRITE_ATTRIBUTES,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                None,
                OPEN_EXISTING,
                0,
                None,
            )
            if handle == INVALID_HANDLE_VALUE:
                return
            try:
                create_ft = ns_to_filetime(times["ctime_ns"])
                access_ft = ns_to_filetime(times["atime_ns"])
                write_ft = ns_to_filetime(times["mtime_ns"])
                ctypes.windll.kernel32.SetFileTime(
                    handle,
                    ctypes.byref(create_ft),
                    ctypes.byref(access_ft),
                    ctypes.byref(write_ft),
                )
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)
        except Exception:
            pass

    def _is_marked_transcoded(self, path: Path) -> bool:
        return path.stem.endswith("_AVC")

    def _cleanup_temp_file(self, path: Path):
        try:
            if path.exists():
                path.unlink()
        except Exception:
            return
        # 尝试向上清理空目录，避免 input/output 目录层级堆积
        try:
            p = path.parent
            for _ in range(6):
                if not p.exists():
                    break
                if any(p.iterdir()):
                    break
                p.rmdir()
                p = p.parent
        except Exception:
            pass

    def _parse_ffmpeg_time_seconds(self, line: str):
        m = re.search(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)", line)
        if not m:
            return None
        try:
            hh = int(m.group(1))
            mm = int(m.group(2))
            ss = float(m.group(3))
            return hh * 3600 + mm * 60 + ss
        except Exception:
            return None

    def _format_eta(self, seconds):
        if seconds is None or seconds < 0:
            return "--:--:--"
        total = int(seconds)
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _run_ffmpeg_with_progress(self, cmd, duration_sec, progress_cb):
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
            bufsize=1,
        )
        log_lines = []
        if proc.stdout is not None:
            for line in proc.stdout:
                line = (line or "").strip()
                if line:
                    log_lines.append(line)
                    if len(log_lines) > 200:
                        log_lines = log_lines[-200:]
                if line.startswith("out_time_ms="):
                    if duration_sec and duration_sec > 0:
                        try:
                            out_ms = int(line.split("=", 1)[1])
                            cur = out_ms / 1_000_000.0
                            progress_cb(min(99.0, max(0.0, cur * 100.0 / duration_sec)))
                        except Exception:
                            pass
                elif line == "progress=end":
                    progress_cb(100.0)
        code = proc.wait()
        progress_cb(100.0)
        return code, "\n".join(log_lines)

    def _set_transcode_file_progress(self, val, text):
        self.transcode_file_progress["value"] = val
        self.transcode_file_progress_label.config(text=text)

    def _set_transcode_progress_with_eta(self, file_pct, file_name, idx, total, batch_start):
        self._set_transcode_file_progress(file_pct, f"当前文件进度: {file_pct:.1f}% ({file_name})")
        overall = ((idx - 1) + max(0.0, min(100.0, file_pct)) / 100.0) * 100.0 / max(1, total)
        self.transcode_progress["value"] = overall
        elapsed = time.time() - batch_start
        eta = None
        if overall > 0:
            eta = elapsed * (100.0 - overall) / overall
        self.transcode_progress_label.config(
            text=f"处理 {idx}/{total}，总进度 {overall:.1f}% ，预计剩余 {self._format_eta(eta)}"
        )

    def _do_transcode(self, src_dir: Path, temp_dir: Path, params):
        files = self._scan_videos([str(src_dir)])
        total = len(files)
        if total == 0:
            self.after(0, lambda: self.transcode_progress_label.config(text="未找到视频文件"))
            self.after(0, self._set_transcode_run_buttons, False)
            return

        temp_in_root = temp_dir / "input"
        temp_out_root = temp_dir / "output"
        temp_in_root.mkdir(parents=True, exist_ok=True)
        temp_out_root.mkdir(parents=True, exist_ok=True)
        temp_dir.mkdir(parents=True, exist_ok=True)

        start_idx = self._find_resume_index(files, temp_dir, src_dir)
        min_size_bytes = params["min_size_gb"] * 1024 * 1024 * 1024
        target_res = self._target_resolution(params.get("resolution", ""))
        target_br = self._target_bitrate_bps(params)

        success = 0
        failed = 0
        skipped = 0
        batch_start = time.time()

        for idx, src_file in enumerate(files, 1):
            if idx - 1 < start_idx:
                continue

            if self._is_marked_transcoded(src_file):
                skipped += 1
                self._log(f"[T]跳过(已带_AVC标识): {src_file}")
                self.after(0, self._set_transcode_progress, idx * 100 / total, f"已完成 {idx}/{total}")
                self.after(0, self._set_transcode_file_progress, 100, f"当前文件已跳过: {src_file.name}")
                continue

            rel = src_file.relative_to(src_dir)
            temp_base = self._safe_temp_name(src_dir, rel)
            temp_in = temp_in_root / f"{temp_base}{src_file.suffix}.tmp"
            temp_out_tmp = temp_out_root / f"{temp_base}.tmp"
            temp_ready = temp_out_root / rel

            temp_in_root.mkdir(parents=True, exist_ok=True)
            temp_out_tmp.parent.mkdir(parents=True, exist_ok=True)
            temp_ready.parent.mkdir(parents=True, exist_ok=True)

            self.after(0, self._set_transcode_progress, (idx - 1) * 100 / total, f"处理 {idx}/{total}: {src_file.name}")
            self.after(0, self._set_transcode_file_progress, 0, f"当前文件进度: 0% ({src_file.name})")
            self._log(f"[T]开始: {src_file}")

            try:
                try:
                    src_size = src_file.stat().st_size
                except OSError:
                    src_size = 0
                if src_size > 0 and src_size < min_size_bytes:
                    skipped += 1
                    self._log(f"[T]跳过(体积小于{params['min_size_gb']}GB): {src_file}")
                    self.after(0, self._set_transcode_progress, idx * 100 / total, f"已完成 {idx}/{total}")
                    self.after(0, self._set_transcode_file_progress, 100, f"当前文件已跳过: {src_file.name}")
                    continue

                src_meta = self._get_video_numeric_meta(src_file)
                src_times = self._read_file_times(src_file)
                if src_meta:
                    low_res_ok = False
                    low_br_ok = False
                    if target_res:
                        low_res_ok = src_meta["width"] <= target_res[0] and src_meta["height"] <= target_res[1]
                    if target_br:
                        low_br_ok = src_meta["bitrate"] > 0 and src_meta["bitrate"] <= target_br
                    # 仅在有明确目标码率时执行“已低于目标参数”跳过。
                    # CRF 模式没有固定目标码率，不应仅凭分辨率就跳过。
                    should_skip = False
                    if target_br and target_res:
                        should_skip = low_res_ok and low_br_ok
                    elif target_br and not target_res:
                        should_skip = low_br_ok
                    if should_skip:
                        skipped += 1
                        self._log(f"[T]跳过(已低于目标参数): {src_file}")
                        self.after(0, self._set_transcode_progress, idx * 100 / total, f"已完成 {idx}/{total}")
                        self.after(0, self._set_transcode_file_progress, 100, f"当前文件已跳过: {src_file.name}")
                        continue

                shutil.copy2(src_file, temp_in)

                cmd = [
                    "ffmpeg",
                    "-y",
                    "-progress",
                    "pipe:1",
                    "-nostats",
                    "-i",
                    str(temp_in),
                    "-c:v",
                    params["codec"],
                ]
                if params["codec"].endswith("_amf"):
                    cmd.extend(["-usage", "transcoding", "-quality", "speed"])
                elif params["codec"].endswith("_nvenc"):
                    cmd.extend(["-preset", "p5"])

                resolution = params["resolution"]
                if resolution != "保持原分辨率":
                    cmd.extend(["-vf", f"scale={resolution}"])

                if params["rate_mode"] == "CRF":
                    preset_value = "medium"
                    # libsvtav1 使用数字 preset，字符串会报错
                    if params["codec"] == "libsvtav1":
                        preset_value = "6"
                    cmd.extend(["-preset", preset_value, "-crf", params["crf"]])
                else:
                    br = params["bitrate"]
                    cmd.extend(["-b:v", br, "-maxrate", br, "-bufsize", "2M"])

                cmd.extend(["-c:a", "aac", "-movflags", "+faststart", "-f", "mp4", str(temp_out_tmp)])

                duration_sec = src_meta["duration"] if src_meta and src_meta.get("duration") else 0
                ret, stderr_text = self._run_ffmpeg_with_progress(
                    cmd,
                    duration_sec,
                    lambda p, n=src_file.name, i=idx: self.after(
                        0, self._set_transcode_progress_with_eta, p, n, i, total, batch_start
                    ),
                )
                if ret != 0:
                    raise RuntimeError((stderr_text or "")[-400:])

                if not self._verify_playable(temp_out_tmp):
                    raise RuntimeError("转码输出验证失败，疑似不可播放")

                # 转码完成后先改回源文件名，再回写到源目录
                if temp_ready.exists():
                    temp_ready.unlink()
                temp_out_tmp.rename(temp_ready)

                target = src_file.with_name(f"{src_file.stem}_AVC{src_file.suffix}")
                if target.exists():
                    target.unlink()
                if src_file.exists():
                    src_file.unlink()
                shutil.move(str(temp_ready), str(target))
                self._set_file_times(target, src_times)

                success += 1
                self._log(f"[T]完成: {target}")
                try:
                    new_size = target.stat().st_size
                except OSError:
                    new_size = 0
                if src_size > 0 and new_size > 0:
                    ratio = new_size * 100.0 / src_size
                    self._log(
                        f"[T]体积变化: 原 {self._format_size(src_size)} -> 新 {self._format_size(new_size)} "
                        f"({ratio:.1f}% of original)"
                    )
                self.after(0, self._set_transcode_file_progress, 100, f"当前文件完成: {src_file.name}")
            except Exception as e:
                failed += 1
                self._log(f"[T]失败: {src_file} ({e})")
                self.after(0, self._set_transcode_file_progress, 100, f"当前文件失败: {src_file.name}")
            finally:
                # 每个文件结束后清理临时文件
                self._cleanup_temp_file(temp_in)
                self._cleanup_temp_file(temp_out_tmp)
                self._cleanup_temp_file(temp_ready)

            self.after(0, self._set_transcode_progress, idx * 100 / total, f"已完成 {idx}/{total}")

            if self.transcode_stop_event.is_set():
                next_file_name = files[idx].name if idx < len(files) else ""
                self._clear_resume_markers(temp_dir, src_dir)
                if next_file_name:
                    marker = self._resume_marker_path(temp_dir, src_dir, next_file_name)
                    marker.touch()
                    self._log(f"[T]已停止，断点标记: {marker.name}")
                self.after(
                    0,
                    lambda: self.transcode_progress_label.config(
                        text=f"已停止，成功 {success}，失败 {failed}，跳过 {skipped}"
                    ),
                )
                self.after(0, self._set_transcode_file_progress, 0, "当前文件：已停止")
                self.after(0, self._set_transcode_run_buttons, False)
                return

        self._clear_resume_markers(temp_dir, src_dir)

        self.after(
            0,
            lambda: self.transcode_progress_label.config(text=f"转码完成，成功 {success}，失败 {failed}，跳过 {skipped}"),
        )
        self.after(0, self._set_transcode_file_progress, 0, "当前文件：等待开始")
        self.after(0, self._set_transcode_run_buttons, False)

    def _verify_playable(self, path: Path) -> bool:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ]
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, check=True)
            duration = float((p.stdout or "0").strip())
            return duration > 0
        except Exception:
            return False

    def _set_transcode_progress(self, val, text):
        self.transcode_progress["value"] = val
        self.transcode_progress_label.config(text=text)

    def _setup_styles(self):
        style = ttk.Style(self)
        style.configure("Delete.TButton")
        # 某些主题下 disabled 文本过浅，这里提高可见度
        style.map("Delete.TButton", foreground=[("disabled", "#666666"), ("!disabled", "#000000")])

    def _apply_windows_scaling(self):
        if os.name != "nt":
            return
        try:
            dpi = float(self.winfo_fpixels("1i"))
            if dpi > 0:
                self.tk.call("tk", "scaling", dpi / 72.0)
        except Exception:
            pass

    def _set_initial_window_size(self):
        self.update_idletasks()
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        width = 1200
        height = 920
        x = max(0, (sw - width) // 2)
        y = max(0, (sh - height) // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")
        self.minsize(880, 580)


def _enable_windows_dpi_awareness():
    if os.name != "nt":
        return
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
        return
    except Exception:
        pass
    try:
        ctypes.windll.user32.SetProcessDPIAware()
    except Exception:
        pass


if __name__ == "__main__":
    _enable_windows_dpi_awareness()
    app = App()
    app.mainloop()
