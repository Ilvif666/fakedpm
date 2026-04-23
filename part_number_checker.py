from __future__ import annotations

import argparse
import errno
import html
import os
import re
import sys
import urllib.parse
import webbrowser
from collections import defaultdict
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
CODE_PART_RE = re.compile(
    r"^(?:\d+-)?\d+\.[^.]+\.(?P<decimal_number>\d+(?:\.\d+)+)(?:-\d+)?$",
    re.IGNORECASE,
)
FIRST_TOKEN_RE = re.compile(r"^\W*(\w+)", re.IGNORECASE)
WINDOWS_DRIVE_RE = re.compile(r"^(?P<drive>[a-zA-Z]):[\\/]*(?P<tail>.*)$")


@dataclass(frozen=True)
class FileEntry:
    decimal_number: str
    detail_text: str
    detail_key: str
    extension: str
    name: str
    path: Path
    relative_path: str


@dataclass(frozen=True)
class NumberGroup:
    decimal_number: str
    entries: list[FileEntry]
    detail_keys: list[str]
    is_suspect: bool


@dataclass(frozen=True)
class DisplayEntry:
    detail_key: str
    extensions: list[str]
    entries: list[FileEntry]


@dataclass(frozen=True)
class ScanResult:
    root: Path
    total_files: int
    checked_files: int
    ignored_files: int
    occupied_count: int
    suspect_count: int
    max_decimal_number: str | None
    next_decimal_number: str | None
    groups: list[NumberGroup]
    skipped_dirs: list[tuple[Path, str]]
    error: str | None = None


def extract_decimal_and_detail(path: Path) -> tuple[str, str]:
    stem = path.stem.strip()
    if not stem:
        return "", ""

    parts = stem.split(maxsplit=1)
    code_part = parts[0]
    detail_text = parts[1].strip() if len(parts) > 1 else ""
    match = CODE_PART_RE.match(code_part)
    return (match.group("decimal_number"), detail_text) if match else ("", detail_text)


def detail_key(detail_text: str) -> str:
    text = detail_text.strip().lower().replace("х", "x")
    match = FIRST_TOKEN_RE.search(text)
    if not match:
        return ""

    token = match.group(1)
    base = re.sub(r"\d.*$", "", token).strip("_-")
    return base or token


def decimal_sort_key(value: str) -> tuple[tuple[int, ...], str]:
    base, suffix = split_execution_suffix(value)
    return tuple(int(part) for part in base.split(".")), suffix, value


def split_execution_suffix(value: str) -> tuple[str, int]:
    if "-" not in value:
        return value, 0

    base, suffix = value.rsplit("-", 1)
    if not suffix.isdigit():
        return value, 0
    return base, int(suffix)


def make_next_decimal_number(value: str | None) -> str | None:
    if not value:
        return None

    base, _suffix = split_execution_suffix(value)
    parts = base.split(".")
    last_width = len(parts[-1])
    parts[-1] = str(int(parts[-1]) + 1).zfill(last_width)
    return ".".join(parts)


def empty_result(root: Path, error: str) -> ScanResult:
    return ScanResult(
        root=root,
        total_files=0,
        checked_files=0,
        ignored_files=0,
        occupied_count=0,
        suspect_count=0,
        max_decimal_number=None,
        next_decimal_number=None,
        groups=[],
        skipped_dirs=[],
        error=error,
    )


def parse_drive_maps(values: list[str]) -> dict[str, str]:
    drive_maps: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"Неверный формат маппинга диска: {value!r}. Нужно вроде S=/mnt/timez_s")

        drive, mount_point = value.split("=", 1)
        drive = drive.strip().rstrip(":").upper()
        mount_point = mount_point.strip()
        if len(drive) != 1 or not drive.isalpha() or not mount_point:
            raise ValueError(f"Неверный формат маппинга диска: {value!r}. Нужно вроде S=/mnt/timez_s")

        drive_maps[drive] = mount_point.replace("\\", "/").rstrip("/")

    return drive_maps


def normalize_root_text(root_text: str, drive_maps: dict[str, str]) -> str:
    match = WINDOWS_DRIVE_RE.match(root_text.strip())
    if not match:
        return root_text

    mount_point = drive_maps.get(match.group("drive").upper())
    if mount_point is None:
        return root_text

    tail = match.group("tail").replace("\\", "/").strip("/")
    return f"{mount_point}/{tail}" if tail else mount_point


def iter_files(root: Path) -> tuple[list[Path], list[tuple[Path, str]]]:
    files: list[Path] = []
    skipped_dirs: list[tuple[Path, str]] = []

    def on_error(exc: OSError) -> None:
        skipped_dirs.append((Path(exc.filename or root), str(exc)))

    for current_root, _dirnames, filenames in os.walk(root, topdown=True, onerror=on_error):
        current_path = Path(current_root)
        for filename in filenames:
            files.append(current_path / filename)

    return files, skipped_dirs


def scan(root_text: str, drive_maps: dict[str, str] | None = None) -> ScanResult:
    normalized_root_text = normalize_root_text(root_text, drive_maps or {})
    root = Path(normalized_root_text).expanduser()

    try:
        root = root.resolve(strict=True)
    except OSError as exc:
        return empty_result(root, str(exc))

    if not root.is_dir():
        return empty_result(root, "Указанный путь не является папкой.")

    files, skipped_dirs = iter_files(root)
    entries_by_number: dict[str, list[FileEntry]] = defaultdict(list)
    checked_files = 0
    ignored_files = 0

    for path in files:
        decimal_number, detail_text = extract_decimal_and_detail(path)
        if not decimal_number:
            ignored_files += 1
            continue

        checked_files += 1
        try:
            relative_path = str(path.relative_to(root))
        except ValueError:
            relative_path = str(path)

        entries_by_number[decimal_number].append(
            FileEntry(
                decimal_number=decimal_number,
                detail_text=detail_text,
                detail_key=detail_key(detail_text),
                extension=path.suffix.lower(),
                name=path.name,
                path=path,
                relative_path=relative_path,
            )
        )

    groups: list[NumberGroup] = []
    for decimal_number, entries in entries_by_number.items():
        sorted_entries = sorted(entries, key=lambda item: (item.detail_key, item.extension, item.relative_path.lower()))
        keys = sorted({entry.detail_key or "(без имени)" for entry in entries})
        groups.append(
            NumberGroup(
                decimal_number=decimal_number,
                entries=sorted_entries,
                detail_keys=keys,
                is_suspect=len(entries) > 1 and len(keys) > 1,
            )
        )

    groups.sort(key=lambda item: decimal_sort_key(item.decimal_number))
    max_decimal_number = max(entries_by_number, key=decimal_sort_key) if entries_by_number else None

    return ScanResult(
        root=root,
        total_files=len(files),
        checked_files=checked_files,
        ignored_files=ignored_files,
        occupied_count=len(entries_by_number),
        suspect_count=sum(1 for group in groups if group.is_suspect),
        max_decimal_number=max_decimal_number,
        next_decimal_number=make_next_decimal_number(max_decimal_number),
        groups=groups,
        skipped_dirs=skipped_dirs,
    )


def as_file_url(path: Path) -> str:
    return path.resolve().as_uri()


def escape(value: object) -> str:
    return html.escape(str(value), quote=True)


def display_name_key(entry: FileEntry) -> str:
    return entry.detail_text.strip().lower().replace("х", "x")


def make_display_entries(group: NumberGroup, compact: bool) -> list[DisplayEntry]:
    entries_by_name: dict[str, list[FileEntry]] = defaultdict(list)
    for entry in group.entries:
        entries_by_name[display_name_key(entry)].append(entry)

    display_entries: list[DisplayEntry] = []
    for entries in entries_by_name.values():
        sorted_entries = sorted(entries, key=lambda item: (item.extension, item.relative_path.lower()))
        extensions = sorted({entry.extension or "без расширения" for entry in sorted_entries})
        display_entries.append(
            DisplayEntry(
                detail_key=sorted_entries[0].detail_key or "(без имени)",
                extensions=extensions,
                entries=sorted_entries,
            )
        )

    display_entries.sort(key=lambda item: (item.detail_key, ", ".join(item.extensions), item.entries[0].relative_path.lower()))

    if not compact:
        return display_entries

    if len(display_entries) <= 1:
        return []

    repeated_name_entries = [entry for entry in display_entries if len(entry.entries) > 1]
    return repeated_name_entries or display_entries


def render_group(group: NumberGroup, compact: bool) -> str:
    display_entries = make_display_entries(group, compact)
    if not display_entries:
        return ""

    state_class = "suspect" if group.is_suspect else "ok"
    state_text = "проверить" if group.is_suspect else "варианты"
    key_text = ", ".join(group.detail_keys)
    rows = []

    for display_entry in display_entries:
        file_links = "<br>".join(
            f'<a href="{escape(as_file_url(entry.path))}">{escape(entry.name)}</a>' for entry in display_entry.entries
        )
        paths = "<br>".join(escape(entry.relative_path) for entry in display_entry.entries)
        rows.append(
            f"""
            <tr>
                <td>{escape(display_entry.detail_key)}</td>
                <td>{escape(", ".join(display_entry.extensions))}</td>
                <td>{file_links}</td>
                <td>{paths}</td>
            </tr>
            """
        )

    return f"""
    <section class="number-group {state_class}">
        <div class="group-head">
            <h2>{escape(group.decimal_number)}</h2>
            <span>{escape(state_text)}</span>
        </div>
        <p class="keys">Основа имени: {escape(key_text)}</p>
        <table>
            <thead>
                <tr>
                    <th>Основа</th>
                    <th>Расширения</th>
                    <th>Файлы</th>
                    <th>Путь внутри папки</th>
                </tr>
            </thead>
            <tbody>{''.join(rows)}</tbody>
        </table>
    </section>
    """


def render_page(result: ScanResult, current_root: str, compact: bool) -> bytes:
    status_class = "bad" if result.error or result.suspect_count else "good"
    status_text = "Есть подозрительные дубли" if result.suspect_count else "Явных дублей нет"
    if result.error:
        status_text = "Не удалось проверить папку"

    max_number = result.max_decimal_number or "не найден"
    next_number = result.next_decimal_number or "не найден"
    error_block = f'<p class="error">{escape(result.error)}</p>' if result.error else ""
    empty_block = ""
    if not result.error and not result.groups:
        empty_block = '<section class="empty">Файлы с децимальными номерами не найдены.</section>'

    rendered_groups = [render_group(group, compact) for group in result.groups]
    visible_groups = [group_html for group_html in rendered_groups if group_html]
    groups_block = "".join(visible_groups)
    if not result.error and result.groups and not visible_groups:
        empty_block = '<section class="empty">Все найденные позиции скрыты фильтром.</section>'

    compact_checked = " checked" if compact else ""
    skipped_rows = []
    for path, reason in result.skipped_dirs:
        skipped_rows.append(
            f"""
            <tr>
                <td>{escape(path)}</td>
                <td>{escape(reason)}</td>
            </tr>
            """
        )

    skipped_block = ""
    if skipped_rows:
        skipped_block = f"""
        <section class="panel">
            <h2>Пропущенные папки</h2>
            <table>
                <thead>
                    <tr>
                        <th>Папка</th>
                        <th>Причина</th>
                    </tr>
                </thead>
                <tbody>{''.join(skipped_rows)}</tbody>
            </table>
        </section>
        """

    html_text = f"""<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Проверка децимальных номеров</title>
    <style>
        :root {{
            color-scheme: light;
            --bg: #f5f7f8;
            --ink: #182023;
            --muted: #5d6b70;
            --line: #d9e0e3;
            --panel: #ffffff;
            --good: #19724a;
            --bad: #b42828;
            --bad-bg: #fff1f1;
            --accent: #175f83;
        }}

        * {{
            box-sizing: border-box;
        }}

        body {{
            margin: 0;
            background: var(--bg);
            color: var(--ink);
            font: 16px/1.45 "Segoe UI", Arial, sans-serif;
        }}

        main {{
            width: min(1180px, calc(100% - 32px));
            margin: 28px auto 44px;
        }}

        h1 {{
            margin: 0 0 18px;
            font-size: 28px;
            font-weight: 700;
        }}

        form,
        .summary,
        .panel,
        .number-group,
        .empty {{
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 8px;
            box-shadow: 0 1px 2px rgb(0 0 0 / 4%);
        }}

        form {{
            display: grid;
            grid-template-columns: minmax(260px, 1fr) auto;
            gap: 12px;
            align-items: end;
            padding: 16px;
        }}

        label {{
            display: grid;
            gap: 6px;
            color: var(--muted);
            font-size: 13px;
            font-weight: 600;
        }}

        .check-label {{
            grid-column: 1 / -1;
            display: flex;
            gap: 9px;
            align-items: center;
            color: var(--ink);
            font-size: 14px;
            font-weight: 600;
        }}

        .check-label input {{
            width: 18px;
            min-height: 18px;
            margin: 0;
            accent-color: var(--accent);
        }}

        input {{
            width: 100%;
            min-height: 40px;
            border: 1px solid #b8c4c9;
            border-radius: 6px;
            color: var(--ink);
            font: inherit;
            padding: 8px 10px;
        }}

        button {{
            min-height: 40px;
            border: 0;
            border-radius: 6px;
            background: var(--accent);
            color: #fff;
            cursor: pointer;
            font: inherit;
            font-weight: 700;
            padding: 8px 16px;
        }}

        .summary {{
            display: grid;
            grid-template-columns: repeat(4, minmax(150px, 1fr));
            gap: 14px;
            margin: 16px 0;
            padding: 16px;
        }}

        .metric {{
            display: grid;
            gap: 3px;
        }}

        .metric strong {{
            font-size: 22px;
        }}

        .metric span,
        .keys {{
            color: var(--muted);
            font-size: 13px;
        }}

        .state strong {{
            color: var(--good);
        }}

        .state.bad strong {{
            color: var(--bad);
        }}

        .error {{
            margin: 16px 0;
            color: var(--bad);
            font-weight: 700;
        }}

        .panel,
        .number-group,
        .empty {{
            margin-top: 14px;
            padding: 16px;
        }}

        .number-group.suspect {{
            background: var(--bad-bg);
            border-color: #e0a0a0;
        }}

        .group-head {{
            display: flex;
            gap: 10px;
            align-items: baseline;
            justify-content: space-between;
        }}

        .group-head h2,
        .panel h2 {{
            margin: 0;
            font-size: 20px;
        }}

        .group-head span {{
            border-radius: 999px;
            color: #fff;
            background: var(--good);
            font-size: 12px;
            font-weight: 700;
            padding: 3px 9px;
            white-space: nowrap;
        }}

        .suspect .group-head span {{
            background: var(--bad);
        }}

        .keys {{
            margin: 6px 0 12px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}

        th,
        td {{
            border-top: 1px solid var(--line);
            padding: 9px 8px;
            text-align: left;
            vertical-align: top;
            word-break: break-word;
        }}

        th {{
            color: var(--muted);
            font-size: 13px;
            font-weight: 700;
        }}

        a {{
            color: var(--accent);
            font-weight: 700;
            text-decoration: none;
        }}

        a:hover {{
            text-decoration: underline;
        }}

        .empty {{
            color: var(--good);
            font-weight: 700;
        }}

        @media (max-width: 760px) {{
            main {{
                width: min(100% - 20px, 1180px);
                margin-top: 16px;
            }}

            form,
            .summary {{
                grid-template-columns: 1fr;
            }}

            h1 {{
                font-size: 23px;
            }}
        }}
    </style>
</head>
<body>
    <main>
        <h1>Проверка децимальных номеров файлов</h1>

        <form method="get" action="/">
            <label>
                Папка
                <input name="root" value="{escape(current_root)}" placeholder="C:\\Проекты\\ТЧ-65">
            </label>
            <button type="submit">Проверить</button>
            <input type="hidden" name="compact" value="0">
            <label class="check-label">
                <input type="checkbox" name="compact" value="1"{compact_checked} onchange="this.form.submit()">
                Скрывать одиночные позиции и одинаковые имена с разными расширениями
            </label>
        </form>

        <section class="summary">
            <div class="metric state {status_class}">
                <strong>{escape(status_text)}</strong>
                <span>результат проверки</span>
            </div>
            <div class="metric">
                <strong>{escape(next_number)}</strong>
                <span>следующий после максимального</span>
            </div>
            <div class="metric">
                <strong>{escape(max_number)}</strong>
                <span>максимальный найденный номер</span>
            </div>
            <div class="metric">
                <strong>{escape(result.suspect_count)}</strong>
                <span>подозрительных номеров</span>
            </div>
        </section>

        {error_block}
        {empty_block}

        <section class="summary">
            <div class="metric">
                <strong>{escape(result.occupied_count)}</strong>
                <span>занятых децимальных номеров</span>
            </div>
            <div class="metric">
                <strong>{escape(result.checked_files)}</strong>
                <span>файлов с номером</span>
            </div>
            <div class="metric">
                <strong>{escape(result.ignored_files)}</strong>
                <span>файлов без номера по шаблону</span>
            </div>
            <div class="metric">
                <strong>{escape(result.total_files)}</strong>
                <span>файлов всего в папке</span>
            </div>
        </section>

        {groups_block}
        {skipped_block}
    </main>
</body>
</html>"""
    return html_text.encode("utf-8")


class CheckerHandler(BaseHTTPRequestHandler):
    server: "CheckerServer"

    def do_GET(self) -> None:
        parsed_url = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed_url.query)
        root = query.get("root", [self.server.default_root])[0]
        compact_values = query.get("compact")
        compact = compact_values is None or "1" in compact_values
        result = scan(root, self.server.drive_maps)
        body = render_page(result, root, compact)

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))


class CheckerServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        default_root: str,
        drive_maps: dict[str, str],
    ) -> None:
        super().__init__(server_address, handler_class)
        self.default_root = default_root
        self.drive_maps = drive_maps


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Локальный веб-сервер для проверки децимальных номеров файлов.",
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=str(Path.cwd()),
        help="Папка для рекурсивной проверки. По умолчанию текущая папка.",
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Адрес сервера. По умолчанию {DEFAULT_HOST}.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Порт сервера. По умолчанию {DEFAULT_PORT}.")
    parser.add_argument("--no-browser", action="store_true", help="Не открывать браузер автоматически.")
    parser.add_argument(
        "--drive-map",
        action="append",
        default=[],
        metavar="S=/mnt/timez_s",
        help="Незаметно заменять Windows-диск на Linux mountpoint. Можно указывать несколько раз.",
    )
    parser.add_argument("--ext", default="", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        drive_maps = parse_drive_maps(args.drive_map)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    server = None
    last_error: OSError | None = None

    for port in range(args.port, args.port + 20):
        try:
            server = CheckerServer((args.host, port), CheckerHandler, args.root, drive_maps)
            break
        except OSError as exc:
            last_error = exc
            if exc.errno not in {errno.EADDRINUSE, 10048}:
                raise

    if server is None:
        print(f"Не удалось запустить сервер: {last_error}", file=sys.stderr)
        return 1

    actual_host, actual_port = server.server_address
    url = f"http://{actual_host}:{actual_port}/?{urllib.parse.urlencode({'root': args.root})}"

    print(f"Проверка файлов запущена: {url}")
    print("Остановить сервер: Ctrl+C")

    if not args.no_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nСервер остановлен.")
    finally:
        server.server_close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
