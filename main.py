from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
# 读取 templates 文件夹里的html
templates = Jinja2Templates(directory="templates")

online_clients = []

@app.get("/")
async def get_index(request: Request):
    # 修正参数顺序
    return templates.TemplateResponse(
        request,
        "chat.html",
        {"request": request}
    )

@app.websocket("/ws")
async def chat(websocket: WebSocket):
    await websocket.accept()
    online_clients.append(websocket)
    print("有人进入聊天室，当前在线人数：", len(online_clients))
    try:
        while True:
            msg = await websocket.receive_text()
            # 只发给其他用户，跳过自己
            for client in online_clients:
                if client != websocket:  # 新增判断：不发给发送者本人
                    await client.send_text(msg)
    except WebSocketDisconnect:
        online_clients.remove(websocket)
        print("有人离开聊天室，剩余在线：", len(online_clients))