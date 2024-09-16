const  {  Schema, model } = require("mongoose")
const mongoose = require('mongoose');  

const videoSchema = new Schema(
  {
    title: { type: String, required: true },
    description: { type: String, required: true },
    views: { type: Number, default: 0 },
    playlist: { type: mongoose.Types.ObjectId, default: null },
    url: { type: String, default: null },
    thumbnail:{
      type:String,default:undefined
    }
  },
  {
    timestamps: true,
  }
);

const Video = model("Video", videoSchema);
module.exports = Video

