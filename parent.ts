import ChildProcess from "node:child_process";

function main() {
    const process = ChildProcess.fork('./child.js', {
        stdio: ['pipe', 'pipe', 'pipe', 'ipc']
    })

    process.on('message', message => {
        console.info('[parent] got message', message)
    })

    process.on('exit', code => {
        console.info(`[parent] exit, code: ${code}`)
    })

    process.on('error', error => {
        console.info(`[parent] error: ${error}`)
    })
}

main()
