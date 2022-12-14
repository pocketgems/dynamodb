const path = require('path')

const { LicenseWebpackPlugin } = require('license-webpack-plugin')
const webpack = require('webpack')

module.exports = {
  entry: './src/dynamodb.js',
  output: {
    filename: 'dynamodb.cjs',
    path: path.resolve(__dirname, 'dist'),
    libraryTarget: 'commonjs2'
  },
  mode: 'production',
  target: 'web',
  module: {
    rules: [
      {
        test: /\.js$/,
        use: {
          loader: 'babel-loader',
          options: {
            plugins: [
              '@babel/plugin-proposal-class-properties'
            ]
          }
        }
      }
    ]
  },
  externals: {
    '@pocketgems/schema': './schema.cjs',
    assert: './assert.cjs',
    'amazon-dax-client': 'invalid',
    'aws-sdk': 'invalid'
  },
  plugins: [
    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: JSON.stringify('webpack')
      }
    }),
    new LicenseWebpackPlugin({
      outputFilename: 'dynamodb-licenses.txt',
      unacceptableLicenseTest: (licenseType) => (licenseType === 'GPL')
    })
  ]
}
