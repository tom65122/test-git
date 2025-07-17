import subprocess
import time
import os
import signal
import psutil

class EdgeDebugController:
    def __init__(self, port=9222, user_data_dir=None):
        # Windows系统默认路径，根据实际情况修改
        self.edge_path = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
        self.port = port
        if user_data_dir is None:
            import tempfile
            self.user_data_dir = os.path.join(tempfile.gettempdir(), f"edge_debug_{port}")
        else:
            self.user_data_dir = user_data_dir

        self.process = None
        self.pid = None

    def start(self):
        os.makedirs(self.user_data_dir, exist_ok=True)
        command = [
            self.edge_path,
            f"--remote-debugging-port={self.port}",
            f"--user-data-dir={self.user_data_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--start-maximized",
            "about:blank"
        ]
        try:
            # 启动 Edge 进程
            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )

            time.sleep(3)

            if self.process.poll() is None:
                self.pid = self.process.pid
                print(f"✅ Edge 已启动 | 端口: {self.port} | PID: {self.pid}")
                print(f"用户数据目录: {self.user_data_dir}")
                return True
            else:
                error = self.process.stderr.read().decode()
                raise RuntimeError(f"Edge 启动失败: {error}")

        except Exception as e:
            print(f"❌ 启动失败: {str(e)}")
            return False

    def close(self):
        if self.process is None:
            print("⚠️ 没有正在运行的 Edge 进程")
            return False

        try:
            # 获取整个进程组
            pgid = os.getpgid(self.pid)

            # 终止整个进程组
            os.killpg(pgid, signal.SIGTERM)

            # 等待进程结束
            self.process.wait(timeout=10)

            print(f"✅ Edge 已关闭 | PID: {self.pid}")
            self.process = None
            self.pid = None
            return True

        except Exception as e:
            print(f"❌ 关闭失败: {str(e)}")
            return False

    def is_running(self):
        """
        检查 Edge 是否仍在运行
        """
        if self.process is None:
            return False

        # 检查进程是否仍在运行
        if self.process.poll() is None:
            return True

        # 检查进程组是否还有活动进程
        try:
            pgid = os.getpgid(self.pid)
            return any(p.pid != os.getpid() for p in psutil.process_iter(attrs=['pid']))
        except:
            return False

    def connect_drissionpage(self):
        """
        使用 DrissionPage 连接到已启动的浏览器
        """
        if not self.is_running():
            print("⚠️ Edge 未运行，请先启动")
            return None

        try:
            from DrissionPage import WebPage
            page = WebPage()
            print("✅ 成功连接到 Edge")
            return page
        except ImportError:
            print("❌ 未安装 DrissionPage，请先安装: pip install DrissionPage")
            return None
        except Exception as e:
            print(f"❌ 连接失败: {str(e)}")
            return None