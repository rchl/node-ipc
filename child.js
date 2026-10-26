function main() {
    process.on('message', message => {
        console.info('[child] got message', message)
        if (message && message.type === 'exit') {
            process.exit(0)
        }
        if (message && message.type === 'echo') {
            process.send(message)
        }
        if (message && message.type === 'argv') {
            process.send({ type: 'argv', argv: process.argv.slice(2) })
        }
    })

    process.on('disconnect', () => {
        console.info('[child] disconnect')
        process.exit(0)
    })

    setTimeout(() => process.send({ type: 'hello', text: 'message from child' }), 2000)
}

main()
