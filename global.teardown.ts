import { Server } from "http";

declare interface GlobalTestServer {
    __TEST_SERVER__?: Server
}
module.exports = async () => {
    return new Promise((resolve) => {
        const globalThisWithServer = globalThis as GlobalTestServer;
        if (globalThisWithServer.__TEST_SERVER__) {
            globalThisWithServer.__TEST_SERVER__.close(() => {
                return resolve(void 0);
            });
            console.log("@themost/sql.js Test server is closed");
        } else {
            console.log("@themost/sql.js Test server is not running");
            return resolve(void 0);
        }
    });
    
}