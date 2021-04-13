import tornado.web
import tornado.ioloop
import uuid

dict_sessions = {}


class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        if self.get_secure_cookie("session_id") is None:
            return None
        session_id = self.get_secure_cookie("session_id").decode("utf-8")
        return dict_sessions.get(session_id)


class MainHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        name = tornado.escape.xhtml_escape(self.current_user)
        self.write("Hello, " + name)


class LoginHandler(BaseHandler):
    def get(self):
        self.write('<html><body><form action="/login" method="post">'
                   'Name: <input type="text" name="name">'
                   '<input type="submit" value="Sign in">'
                   '</form></body></html>')

    def post(self):
        if len(self.get_argument("name")) < 3:
            self.redirect("/login")
            return
        session_id = str(uuid.uuid1())
        dict_sessions[session_id] = self.get_argument("name")
        self.set_secure_cookie("session_id", session_id)
        self.redirect("/")


application = tornado.web.Application(
    [
        (r"/", MainHandler),
        (r"/login", LoginHandler),
    ],
    cookie_secret="SECRET_DONT_LEAK",
    login_url="/login")


def main():
    application.listen(8888)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
