interface Command {
    query: string;
    time: number;
}

interface DuckDBApp {
    ws: WebSocket | null;
    currentQuery: string;
    output: string;
    dbName: string;
    previousCommands: Command[];
    searchTerm: string;
    loading: boolean;
    commandIndex: number;
    startTime: number;

    initWebSocket(): void;
    handleMessage(message: string): void;
    handleError(message: string): void;
    isValidQuery(): boolean;
    handleEnterKey(event: KeyboardEvent): void;
    sendQuery(): void;
    setQuery(query: string): void;
    navigateHistory(direction: "up" | "down"): void;
    updateLatestExecutionTime(): void;
    scrollToBottom(): void;
}

function duckdbApp(): DuckDBApp {
    return {
        ws: null,
        currentQuery: "",
        output: "",
        dbName: "",
        previousCommands: [],
        searchTerm: "",
        loading: false,
        commandIndex: -1,
        startTime: 0,

        initWebSocket() {
            const wsProtocol = window.location.protocol === "https:"
                ? "wss:"
                : "ws:";
            const wsUrl = `${wsProtocol}//${location.host}`;
            this.ws = new WebSocket(wsUrl);
            this.ws.onmessage = (event) => {
                const message = JSON.parse(event.data);
                if (message.type === "stdout") {
                    this.handleMessage(message.data);
                } else if (message.type === "stderr") {
                    this.handleError(message.data);
                }
                this.updateLatestExecutionTime();
                this.loading = false;
                this.scrollToBottom();
            };
        },

        handleMessage(message: string) {
            if (message.startsWith("Using database:")) {
                this.dbName = message;
            } else {
                this.output += message + "\n";
            }
        },

        handleError(message: string) {
            this.output += `<span class="text-red-500">${message}</span>\n`;
        },

        isValidQuery() {
            const query = this.currentQuery.trim();
            return query.startsWith(".") || query.endsWith(";");
        },

        handleEnterKey(event: KeyboardEvent) {
            if (!event.shiftKey && this.isValidQuery()) {
                this.sendQuery();
            } else {
                // Insert a newline at the cursor position
                const target = event.target as HTMLTextAreaElement;
                const start = target.selectionStart;
                const end = target.selectionEnd;
                const value = target.value;
                this.currentQuery = value.substring(0, start) + "\n" +
                    value.substring(end);
                // Move cursor to after the inserted newline
                setTimeout(() => {
                    target.selectionStart = target.selectionEnd = start + 1;
                }, 0);
            }
        },

        sendQuery() {
            if (this.isValidQuery() && this.ws) {
                this.startTime = Date.now();
                this.ws.send(this.currentQuery);
                this.output +=
                    `<a href="#" class="text-blue-500 hover:underline" @click.prevent="setQuery('${
                        this.currentQuery.replace(/'/g, "\\'")
                    }')">> ${this.currentQuery}</a>\n`;
                this.previousCommands.unshift({
                    query: this.currentQuery,
                    time: 0,
                });
                this.currentQuery = "";
                this.loading = true;
                this.commandIndex = -1;
                this.scrollToBottom();
            }
        },

        setQuery(query: string) {
            this.currentQuery = query;
        },

        navigateHistory(direction: "up" | "down") {
            if (
                direction === "up" &&
                this.commandIndex < this.previousCommands.length - 1
            ) {
                this.commandIndex++;
            } else if (direction === "down" && this.commandIndex > -1) {
                this.commandIndex--;
            }
            this.currentQuery = this.commandIndex === -1
                ? ""
                : this.previousCommands[this.commandIndex].query;
        },

        updateLatestExecutionTime() {
            if (this.previousCommands.length > 0) {
                this.previousCommands[0].time = (Date.now() - this.startTime) /
                    1000;
            }
        },

        scrollToBottom() {
            const outputElement = document.getElementById("output");
            if (outputElement) {
                outputElement.scrollTop = outputElement.scrollHeight;
            }
        },
    };
}

// @ts-ignore alpinejs loader
globalThis.duckdbApp = duckdbApp;
