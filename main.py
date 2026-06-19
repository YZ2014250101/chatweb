from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates
import html
import asyncio

app = FastAPI()
templates = Jinja2Templates(directory="templates")
online_clients = []

# 基础安全配置
MAX_MSG_LENGTH = 500
MAX_ONLINE = 30
# 节流冷却：350毫秒，平衡实时性与发包频率，可改为0.2~0.6
THROTTLE_DELAY = 0.35
# 节流全局锁：防止短时间多次触发重复广播
push_online_task: asyncio.Task | None = None

# 全局流量统计（字节）
total_upload_bytes = 0   # 服务下发客户端（下行）
total_download_bytes = 0 # 客户端上传服务（上行）

def backend_escape(raw_str: str) -> str:
    """后端二次XSS转义兜底"""
    return html.escape(raw_str)

def format_byte(size: int) -> str:
    """字节格式化 B/KB/MB"""
    if size < 1024:
        return f"{size} B"
    elif size < 1024 * 1024:
        return f"{size / 1024:.2f} KB"
    else:
        return f"{size / (1024 * 1024):.2f} MB"

async def batch_push_online_count():
    """节流冷却完成后，批量推送在线人数给全部客户端"""
    global push_online_task, total_upload_bytes
    push_online_task = None
    current_count = len(online_clients)
    online_cmd = f"#N#{current_count}"
    cmd_byte_len = len(online_cmd.encode("utf-8"))

    print(f"【节流批量推送】当前在线：{current_count} 人，单条指令 {cmd_byte_len} B")
    # 一次性遍历所有连接推送1次，短时间多次上下线只会走这一次循环
    for client in online_clients:
        await client.send_text(online_cmd)
        total_upload_bytes += cmd_byte_len
    print(f"【下行流量更新】累计下行总流量：{format_byte(total_upload_bytes)}")

def trigger_throttle_push():
    """触发节流推送，已有等待任务则直接合并丢弃重复请求"""
    global push_online_task
    if push_online_task is not None:
        # 已有待执行任务，本次变更合并到上一次，不新建任务
        return
    # 创建延时后台任务
    async def delay_wrapper():
        await asyncio.sleep(THROTTLE_DELAY)
        await batch_push_online_count()
    push_online_task = asyncio.create_task(delay_wrapper())

@app.get("/")
async def get_index(request: Request):
    return templates.TemplateResponse(request, "chat.html", {"request": request})

@app.websocket("/ws")
async def chat(websocket: WebSocket):
    global total_upload_bytes, total_download_bytes
    # 拦截超额连接
    if len(online_clients) >= MAX_ONLINE:
        await websocket.close(code=1008, reason="在线人数已满，请稍后重试")
        return

    await websocket.accept()
    online_clients.append(websocket)
    # 触发节流，不单独给新连接发初始化小包
    trigger_throttle_push()

    try:
        while True:
            raw_data = await websocket.receive_text()
            # 统计上行流量
            recv_byte = len(raw_data.encode("utf-8"))
            total_download_bytes += recv_byte
            print(f"【收到聊天消息】上行 {recv_byte} B，累计上行：{format_byte(total_download_bytes)}")

            # 超长消息直接丢弃
            if len(raw_data) > MAX_MSG_LENGTH:
                continue

            safe_text = backend_escape(raw_data)
            msg_byte = len(safe_text.encode("utf-8"))
            # 纯聊天消息广播，不带任何在线人数前缀
            for client in online_clients:
                if client != websocket:
                    await client.send_text(safe_text)
                    total_upload_bytes += msg_byte
            print(f"【广播聊天消息】单条每条 {msg_byte} B，累计下行：{format_byte(total_upload_bytes)}")

    except WebSocketDisconnect:
        online_clients.remove(websocket)
        # 下线同样触发节流合并推送
        trigger_throttle_push()
        # 断开打印全局流量汇总
        print("\n===== 全局双向流量汇总 =====")
        print(f"客户端上行总流量：{format_byte(total_download_bytes)}")
        print(f"服务端下行总流量：{format_byte(total_upload_bytes)}")
        print(f"双向合计流量：{format_byte(total_download_bytes + total_upload_bytes)}")
        print("============================\n")