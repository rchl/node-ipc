function main() {
    process.on('message', message => {
        console.info('[child] got message', message)
        if (message && message.type === 'exit') {
            process.exit(0)
        }
    })

    process.on('disconnect', () => {
        console.info('[child] disconnect')
        process.exit(0)
    })

    setTimeout(() => process.send({ type: 'hello', text: 'message from child' }), 2000)
}

main()
