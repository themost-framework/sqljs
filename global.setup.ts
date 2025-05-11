import { createServer } from "http-server";

module.exports = async () => {
    await new Promise((resolve) => {
        const server = createServer({
            root: "./spec/public",
            cors: true,
        });
        server.listen(3000, () => {
            Object.assign(globalThis, {
                __TEST_SERVER__: server,
                __TEST_SERVER_PORT__: 3000
            });
            console.log("@themost/sql.js Test server is running at http://localhost:3000");
            resolve(void 0);
        });
    })
}