import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol


class PrinterBackend(Protocol):
    """Минимальный интерфейс, который нужен Supervisor."""

    def print_text(self, text: str, *, job_name: str = "report") -> str:
        ...


def _app_dir() -> Path:
    """
    printer.py лежит в app/daemon/.
    Возвращаем папку app.
    """
    return Path(__file__).resolve().parent.parent


def _project_test_prints_dir() -> Path:
    """
    Тестовая печать в файл.

    printer.py лежит:
        app/daemon/printer.py

    test_prints лежит:
        app/test_prints
    """
    return _app_dir() / "test_prints"   


def _default_real_spool_dir() -> Path:
    """
    Spool-папка для штатной Ubuntu-печати.

    printer.py лежит:
        app/daemon/printer.py

    spool-файлы реальной печати лежат:
        printed_documents/

    Структура:
        project/
          app/
            daemon/
              printer.py
          printed_documents/
    """
    return Path(__file__).resolve().parent.parent.parent / "printed_documents"


def _safe_job_name(name: str) -> str:
    """
    Безопасное имя файла для Windows и Linux.
    """
    raw = str(name or "report").strip() or "report"

    forbidden = '<>:"/\\|?*'
    safe = "".join("_" if ch in forbidden else ch for ch in raw)
    safe = "".join(ch if ord(ch) >= 32 else "_" for ch in safe)

    # Убираем хвостовые точки/пробелы, которые проблемны на Windows.
    safe = safe.rstrip(" .")

    return safe[:120] or "report"


class _SpoolWriter:
    """
    Общая логика записи spool-файлов.
    Используется и реальной печатью, и тестовой печатью в файл.
    """

    def __init__(self, spool_dir: str | Path):
        self.spool_dir = Path(spool_dir)
        self.spool_dir.mkdir(parents=True, exist_ok=True)

    def _write_spool(self, prefix: str, text: str) -> str:
        # %f нужен, чтобы несколько документов за одну секунду
        # не перезаписывали друг друга.
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        path = self.spool_dir / f"{_safe_job_name(prefix)}_{ts}.txt"
        path.write_text(str(text or ""), encoding="utf-8")
        return str(path)

    def print_reject_bin_report(self, *, reject_bin_id: int, text: str) -> str:
        return self.print_text(text, job_name=f"rejectbin_{reject_bin_id}")

    def print_batch_report(self, *, batch_id: int, text: str) -> str:
        return self.print_text(text, job_name=f"batch_{batch_id}")


class CupsPrinter(_SpoolWriter):
    """
    Штатная реальная печать для Ubuntu 24.04 через CUPS/lp.

    Поведение:
    - сначала пишет текстовый spool-файл;
    - затем отправляет этот файл в lp;
    - если lp недоступен/упал, daemon не падает;
    - путь к spool-файлу всё равно возвращается.
    """

    def __init__(
        self,
        printer_name: str = "",
        spool_dir: str | Path | None = None,
        *,
        lp_bin: str = "lp",
    ):
        super().__init__(spool_dir or _default_real_spool_dir())
        self.printer_name = str(printer_name or "").strip()
        self.lp_bin = str(lp_bin or "lp").strip() or "lp"

    def print_text(self, text: str, *, job_name: str = "report") -> str:
        path = self._write_spool(job_name, text)

        cmd = [self.lp_bin]
        if self.printer_name:
            cmd += ["-d", self.printer_name]
        cmd += [path]

        try:
            subprocess.run(
                cmd,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            # Печать не должна ронять daemon.
            # Spool-файл сохранён, путь возвращаем.
            pass

        return path


class FilePrinter(_SpoolWriter):
    """
    Тестовая печать только в файл.

    Подходит для:
    - Windows 10;
    - Ubuntu 24.04;
    - локальной отладки без реального принтера.

    По умолчанию пишет в test_prints на два уровня выше папки daemon.
    """

    def __init__(self, spool_dir: str | Path | None = None):
        super().__init__(spool_dir or _project_test_prints_dir())

    def print_text(self, text: str, *, job_name: str = "report") -> str:
        return self._write_spool(job_name, text)


# Совместимость со старым кодом.
# Старый Printer теперь означает реальную печать.
Printer = CupsPrinter

# Старый MockPrinter теперь означает тестовую печать в файл.
MockPrinter = FilePrinter