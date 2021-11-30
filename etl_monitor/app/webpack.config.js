module.exports = {
    entry: {
        app: "./js/App.js",
    },
    watch: true,
    module: {
        rules: [
            {
                test: /\.(js|jsx)$/,
                use: 'babel-loader',
            },
            {
                test: /\.(svg|png|jpg|jpeg|gif)$/,
                loader: "file-loader",
                options: {
                    name: "[name].[ext]",
                    outputPath: "../../dist",
                },
            },
            {
                test: /\.css$/i,
                use: ["style-loader", "css-loader"],
            }
        ],
    },
    optimization: {
        splitChunks: {
            chunks: 'all',
            name: 'commons'
        }
    },
    output: {
        path: __dirname + "/dist",
        filename: "etl_monitor.[name].bundle.js",
    },
};