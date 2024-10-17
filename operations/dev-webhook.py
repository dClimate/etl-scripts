import http.server
import socketserver

class BlackholeHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        print(f"Received POST request: {post_data.decode('utf-8')}")
        self.send_response(200)
        self.end_headers()

if __name__ == "__main__":
    PORT = 8000
    Handler = BlackholeHandler

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"Use URL localhost:{PORT} for DISCORD_WEBHOOK_URL to blackhole the messages")
        httpd.serve_forever()
