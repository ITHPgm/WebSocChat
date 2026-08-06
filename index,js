const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;

// In-memory message history buffer (keeps last 50 messages for new connections)
const MAX_HISTORY = 50;
const messageHistory = [];

// Embedded HTML client interface served directly by the Node.js server
const htmlClient = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Advanced Real-Time WebSocket Chat</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        body { font-family: 'Inter', sans-serif; }
        .custom-scrollbar::-webkit-scrollbar { width: 6px; }
        .custom-scrollbar::-webkit-scrollbar-track { background: rgba(15, 23, 42, 0.6); }
        .custom-scrollbar::-webkit-scrollbar-thumb { background: rgba(51, 65, 85, 0.8); border-radius: 3px; }
        .custom-scrollbar::-webkit-scrollbar-thumb:hover { background: rgba(71, 85, 105, 1); }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 h-screen flex flex-col justify-between overflow-hidden">

    <!-- Header -->
    <header class="bg-slate-900/90 backdrop-blur-md border-b border-slate-800 p-4 flex items-center justify-between shadow-lg z-10">
        <div class="flex items-center space-x-3">
            <div class="relative flex items-center justify-center">
                <div class="w-3.5 h-3.5 rounded-full bg-emerald-500 animate-pulse" id="connectionIndicator"></div>
                <div class="absolute w-5 h-5 rounded-full bg-emerald-500/30 animate-ping"></div>
            </div>
            <div>
                <h1 class="text-base font-bold tracking-tight text-white flex items-center gap-2">
                    Node.js WebSocket Chat 
                    <span class="text-[10px] bg-blue-600/30 text-blue-400 border border-blue-500/30 px-2 py-0.5 rounded-full uppercase tracking-wider">v2.1 Pro</span>
                </h1>
                <p class="text-xs text-slate-400" id="connectionStatusText">Connecting to secure socket...</p>
            </div>
        </div>
        
        <div class="flex items-center space-x-3">
            <div class="text-xs text-slate-300 bg-slate-800/80 px-3.5 py-1.5 rounded-xl border border-slate-700 flex items-center gap-2 shadow-inner">
                <svg class="w-3.5 h-3.5 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"></path></svg>
                <span>Online: <span id="userCount" class="font-bold text-emerald-400">0</span></span>
            </div>
        </div>
    </header>

    <!-- Chat Messages Container -->
    <main id="chatContainer" class="flex-1 overflow-y-auto p-4 space-y-4 max-w-4xl w-full mx-auto custom-scrollbar">
        <div class="text-center my-4">
            <span class="text-xs bg-slate-900/80 border border-slate-800 text-slate-400 px-4 py-1.5 rounded-full shadow-sm">
                💬 Welcome to the real-time encrypted room. Messages sync instantly.
            </span>
        </div>
    </main>

    <!-- Typing Indicator Bar -->
    <div id="typingIndicatorContainer" class="max-w-4xl w-full mx-auto px-6 h-6 flex items-center text-xs text-slate-400 italic transition-all opacity-0">
        <span id="typingText"></span>
    </div>

    <!-- Message Input Footer -->
    <footer class="bg-slate-900/90 backdrop-blur-md border-t border-slate-800 p-4 shadow-2xl z-10">
        <form id="chatForm" class="max-w-4xl mx-auto flex gap-3">
            <input type="text" id="usernameInput" placeholder="Name" required maxlength="20"
                   class="w-28 sm:w-36 px-3.5 py-3 bg-slate-950 border border-slate-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-white placeholder-slate-500 shadow-inner">
            
            <div class="relative flex-1">
                <input type="text" id="messageInput" placeholder="Type your message here..." required autocomplete="off"
                       class="w-full px-4 py-3 bg-slate-950 border border-slate-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-white placeholder-slate-500 shadow-inner pr-10">
            </div>

            <button type="submit" 
                    class="px-6 py-3 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white font-semibold rounded-xl text-sm transition-all shadow-lg shadow-blue-600/30 active:scale-95 flex items-center gap-2">
                <span>Send</span>
                <svg class="w-4 h-4 rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"></path></svg>
            </button>
        </form>
    </footer>

    <!-- Client-side WebSocket Script with Heartbeat/Reconnect Logic -->
    <script>
        const chatContainer = document.getElementById('chatContainer');
        const chatForm = document.getElementById('chatForm');
        const messageInput = document.getElementById('messageInput');
        const usernameInput = document.getElementById('usernameInput');
        const userCountSpan = document.getElementById('userCount');
        const connectionIndicator = document.getElementById('connectionIndicator');
        const connectionStatusText = document.getElementById('connectionStatusText');
        const typingIndicatorContainer = document.getElementById('typingIndicatorContainer');
        const typingText = document.getElementById('typingText');

        // Persist username locally
        const savedUsername = localStorage.getItem('ws_chat_username');
        if (savedUsername) usernameInput.value = savedUsername;

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = \`\${protocol}//\${window.location.host}\`;
        
        let socket = null;
        let reconnectAttempts = 0;
        let pingInterval = null;
        let isTypingSent = false;

        function connectWebSocket() {
            if (socket) {
                socket.close();
            }

            connectionStatusText.textContent = 'Connecting to secure socket...';
            socket = new WebSocket(wsUrl);

            socket.onopen = () => {
                reconnectAttempts = 0;
                connectionIndicator.className = 'w-3.5 h-3.5 rounded-full bg-emerald-500';
                connectionStatusText.textContent = 'Connected & active';
                appendSystemMessage('Secure connection established with server.');

                // Clear any existing ping heartbeat interval
                if (pingInterval) clearInterval(pingInterval);

                // Send a client ping every 25 seconds to keep connection alive through proxies/firewalls
                pingInterval = setInterval(() => {
                    if (socket && socket.readyState === WebSocket.OPEN) {
                        socket.send(JSON.stringify({ type: 'ping' }));
                    }
                }, 25000);
            };

            socket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    
                    if (data.type === 'pong' || data.type === 'ping') {
                        // Heartbeat acknowledgement, ignore display
                        return;
                    }

                    if (data.type === 'history') {
                        chatContainer.innerHTML = \`
                            <div class="text-center my-4">
                                <span class="text-xs bg-slate-900/80 border border-slate-800 text-slate-400 px-4 py-1.5 rounded-full shadow-sm">
                                    💬 Connected to chat room. Recent message history loaded.
                                </span>
                            </div>
                        \`;
                        if (Array.isArray(data.messages)) {
                            data.messages.forEach(msg => {
                                appendChatMessage(msg.username, msg.message, msg.time, msg.isSelf, msg.color);
                            });
                        }
                    } else if (data.type === 'message') {
                        appendChatMessage(data.username, data.message, data.time, data.isSelf, data.color);
                    } else if (data.type === 'notification') {
                        appendSystemMessage(data.message);
                        if (data.clientCount !== undefined) {
                            userCountSpan.textContent = data.clientCount;
                        }
                    } else if (data.type === 'typing') {
                        showTypingIndicator(data.username);
                    }
                } catch (err) {
                    console.error('Failed to parse incoming message:', err);
                }
            };

            socket.onclose = (event) => {
                if (pingInterval) clearInterval(pingInterval);
                connectionIndicator.className = 'w-3.5 h-3.5 rounded-full bg-rose-500 animate-pulse';
                
                reconnectAttempts++;
                const backoffTime = Math.min(1000 * Math.pow(1.5, reconnectAttempts), 10000);
                
                connectionStatusText.textContent = \`Disconnected. Reconnecting in \${Math.round(backoffTime/1000)}s...\`;
                appendSystemMessage('Connection lost. Attempting automatic reconnection...');
                
                setTimeout(connectWebSocket, backoffTime);
            };

            socket.onerror = (error) => {
                console.error('WebSocket error encountered:', error);
            };
        }

        function appendChatMessage(username, message, time, isSelf, colorClass) {
            const isScrolledToBottom = chatContainer.scrollHeight - chatContainer.clientHeight <= chatContainer.scrollTop + 50;
            
            const messageDiv = document.createElement('div');
            messageDiv.className = \`flex flex-col \${isSelf ? 'items-end' : 'items-start'} mb-2 animate-fadeIn\`;

            const badgeColor = colorClass || 'text-blue-400';

            messageDiv.innerHTML = \`
                <div class="flex items-center space-x-2 mb-1 px-1">
                    <span class="text-xs font-semibold \${badgeColor}">\${escapeHtml(username)}</span>
                    <span class="text-[10px] text-slate-500">\${escapeHtml(time)}</span>
                </div>
                <div class="max-w-[85%] sm:max-w-[75%] px-4 py-2.5 rounded-2xl text-sm \${
                    isSelf 
                        ? 'bg-blue-600 text-white rounded-br-none shadow-md shadow-blue-600/10' 
                        : 'bg-slate-900 text-slate-200 border border-slate-800 rounded-bl-none shadow-sm'
                } break-words leading-relaxed">
                    \${escapeHtml(message)}
                </div>
            \`;

            chatContainer.appendChild(messageDiv);

            if (isScrolledToBottom) {
                chatContainer.scrollTop = chatContainer.scrollHeight;
            }
        }

        function appendSystemMessage(text) {
            const systemDiv = document.createElement('div');
            systemDiv.className = 'text-center my-3';
            systemDiv.innerHTML = \`<span class="text-xs bg-slate-900/90 border border-slate-800/80 text-slate-400 px-3.5 py-1.5 rounded-full shadow-sm">\${escapeHtml(text)}</span>\`;
            chatContainer.appendChild(systemDiv);
            chatContainer.scrollTop = chatContainer.scrollHeight;
        }

        let typingHideTimer = null;
        function showTypingIndicator(username) {
            if (!username) return;
            typingText.textContent = \`\${username} is typing a message...\`;
            typingIndicatorContainer.style.opacity = '1';

            clearTimeout(typingHideTimer);
            typingHideTimer = setTimeout(() => {
                typingIndicatorContainer.style.opacity = '0';
            }, 2000);
        }

        messageInput.addEventListener('input', () => {
            const username = usernameInput.value.trim() || 'Anonymous';
            if (!socket || socket.readyState !== WebSocket.OPEN) return;

            if (!isTypingSent) {
                isTypingSent = true;
                socket.send(JSON.stringify({ type: 'typing', username: username }));
                setTimeout(() => { isTypingSent = false; }, 1500);
            }
        });

        function escapeHtml(str) {
            return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        }

        chatForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const message = messageInput.value.trim();
            const username = usernameInput.value.trim() || 'Anonymous';

            if (!message || !socket || socket.readyState !== WebSocket.OPEN) return;

            localStorage.setItem('ws_chat_username', username);

            socket.send(JSON.stringify({
                type: 'message',
                username: username,
                message: message
            }));

            messageInput.value = '';
            messageInput.focus();
        });

        connectWebSocket();
    </script>
</body>
</html>
`;

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(htmlClient);
});

const wss = new WebSocketServer({ server });
const clients = new Map(); // Map client WebSocket to user metadata

// Helper color generator for users
const nameColors = [
    'text-blue-400', 'text-indigo-400', 'text-purple-400', 
    'text-pink-400', 'text-emerald-400', 'text-amber-400', 'text-cyan-400'
];

function getUserColor(username) {
    let hash = 0;
    for (let i = 0; i < username.length; i++) {
        hash = username.charCodeAt(i) + ((hash << 5) - hash);
    }
    return nameColors[Math.abs(hash) % nameColors.length];
}

wss.on('connection', (ws) => {
    ws.isAlive = true;
    clients.set(ws, { username: 'Anonymous' });
    broadcastNotification('A new user connected.', clients.size);

    // Send existing message history to the newly connected client
    if (messageHistory.length > 0) {
        ws.send(JSON.stringify({
            type: 'history',
            messages: messageHistory.map(m => ({
                ...m,
                isSelf: false
            }))
        }));
    }

    // Handle WebSocket Pong / Ping for heartbeat connection keeping
    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (rawMessage) => {
        try {
            const data = JSON.parse(rawMessage.toString());
            
            // Handle client heartbeat ping
            if (data.type === 'ping') {
                ws.isAlive = true;
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'pong' }));
                }
                return;
            }

            if (data.type === 'typing') {
                const username = (data.username && typeof data.username === 'string') 
                    ? data.username.trim().slice(0, 20) 
                    : 'Anonymous';
                
                // Broadcast typing status to everyone except sender
                for (const [client] of clients) {
                    if (client !== ws && client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({
                            type: 'typing',
                            username: username
                        }));
                    }
                }
                return;
            }

            if (!data.message || typeof data.message !== 'string') return;

            const sanitizedMessage = data.message.trim().slice(0, 500);
            const sanitizedUsername = (data.username && typeof data.username === 'string') 
                ? data.username.trim().slice(0, 20) 
                : 'Anonymous';

            clients.set(ws, { username: sanitizedUsername });

            const timeString = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            const userColor = getUserColor(sanitizedUsername);

            const messageRecord = {
                type: 'message',
                username: sanitizedUsername,
                message: sanitizedMessage,
                time: timeString,
                color: userColor
            };

            // Store in history buffer
            messageHistory.push(messageRecord);
            if (messageHistory.length > MAX_HISTORY) {
                messageHistory.shift();
            }

            // Broadcast message to all connected clients with isSelf check
            for (const [client] of clients) {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({
                        ...messageRecord,
                        isSelf: client === ws
                    }));
                }
            }
        } catch (err) {
            console.error('Error processing client message:', err);
        }
    });

    ws.on('close', () => {
        clients.delete(ws);
        broadcastNotification('A user left the chat.', clients.size);
    });

    ws.on('error', (error) => {
        console.error('Socket error:', error);
    });
});

// Periodic heartbeat sweep to clean up dead connections every 30 seconds
const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            return ws.terminate();
        }
        ws.isAlive = false;
        if (typeof ws.ping === 'function') {
            ws.ping();
        }
    });
}, 30000);

wss.on('close', () => {
    clearInterval(heartbeatInterval);
});

function broadcastNotification(text, clientCount) {
    const payload = JSON.stringify({
        type: 'notification',
        message: text,
        clientCount: clientCount
    });

    for (const [client] of clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    }
}

server.listen(PORT, () => {
    console.log('====================================================');
    console.log('🚀 Advanced Node.js WebSocket Chat Server running!');
    console.log(`👉 Open your browser at: http://localhost:${PORT}`);
    console.log('====================================================');
});
