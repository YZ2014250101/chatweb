from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates
import html
import asyncio

app = FastAPI()
templates = Jinja2Templates(directory="templates")
online_clients = []

# 安全配置拆分
MAX_TEXT_LENGTH = 500        # 纯文字消息单条上限
MAX_ALL_MSG_LENGTH = 12000000  # 语音Base64超长兼容总上限
MAX_ONLINE = 30
THROTTLE_DELAY = 0.35
push_online_task: asyncio.Task | None = None

# 全局流量统计（字节）
total_upload_bytes = 0   # 服务下发客户端（下行）
total_download_bytes = 0 # 客户端上传服务（上行）

# 字节格式化工具
def format_byte(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    elif size < 1024 * 1024:
        return f"{size / 1024:.2f} KB"
    else:
        return f"{size / (1024 * 1024):.2f} MB"

# 节流批量推送在线人数
async def batch_push_online_count():
    global push_online_task, total_upload_bytes
    push_online_task = None
    current_count = len(online_clients)
    online_cmd = f"#N#{current_count}"
    cmd_byte_len = len(online_cmd.encode("utf-8"))

    print(f"【节流批量推送】当前在线：{current_count} 人，单条指令 {cmd_byte_len} B")
    for client in online_clients:
        await client.send_text(online_cmd)
        total_upload_bytes += cmd_byte_len
    print(f"【下行流量更新】累计下行总流量：{format_byte(total_upload_bytes)}")

# 触发节流推送，短时间多次上下线合并为一次广播
def trigger_throttle_push():
    global push_online_task
    if push_online_task is not None:
        return
    async def delay_wrapper():
        await asyncio.sleep(THROTTLE_DELAY)
        await batch_push_online_count()
    push_online_task = asyncio.create_task(delay_wrapper())

# 首页路由
@app.get("/")
async def get_index(request: Request):
    return templates.TemplateResponse(request, "chat.html", {"request": request})

# WebSocket聊天主逻辑
@app.websocket("/ws")
async def chat(websocket: WebSocket):
    global total_upload_bytes, total_download_bytes
    # 拦截超额连接
    if len(online_clients) >= MAX_ONLINE:
        await websocket.close(code=1008, reason="在线人数已满，请稍后重试")
        return

    await websocket.accept()
    online_clients.append(websocket)
    trigger_throttle_push()

    try:
        while True:
            raw_data = await websocket.receive_text()
            recv_byte = len(raw_data.encode("utf-8"))
            total_download_bytes += recv_byte

            # 全局超长消息拦截（兼容长语音base64）
            if len(raw_data) > MAX_ALL_MSG_LENGTH:
                print(f"【消息超长丢弃】长度{len(raw_data)}，超出上限{MAX_ALL_MSG_LENGTH}")
                continue

            # 区分消息类型
            if raw_data.startswith("#VOICE#"):
                # 语音消息：原样转发，不做任何转义
                print(f"【收到语音消息】上行 {recv_byte} B，累计上行：{format_byte(total_download_bytes)}")
                send_data = raw_data
            else:
                # 普通文字：前端已完成XSS转义，后端直接原始转发，删除html.escape双重转义
                print(f"【收到文字消息】上行 {recv_byte} B，累计上行：{format_byte(total_download_bytes)}")
                if len(raw_data) > MAX_TEXT_LENGTH:
                    continue
                send_data = raw_data

            # 广播给其他所有在线客户端
            msg_byte = len(send_data.encode("utf-8"))
            for client in online_clients:
                if client != websocket:
                    await client.send_text(send_data)
                    total_upload_bytes += msg_byte
            print(f"【广播下发】单条每条 {msg_byte} B，累计下行：{format_byte(total_upload_bytes)}")

    except WebSocketDisconnect:
        online_clients.remove(websocket)
        trigger_throttle_push()
        # 客户端断开打印完整流量汇总
        print("\n===== 全局双向流量汇总 =====")
        print(f"客户端上行总流量：{format_byte(total_download_bytes)}")
        print(f"服务端下行总流量：{format_byte(total_upload_bytes)}")
        print(f"双向合计流量：{format_byte(total_download_bytes + total_upload_bytes)}")
        print("============================\n")