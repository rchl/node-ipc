function main() {
    process.on('message', message => {
        console.info('[child] got message', message)
    })

    process.on('disconnect', () => {
        console.info('[child] disconnect')
    })

    process.send('message from child')
}

main()
