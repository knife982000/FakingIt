package edu.isistan.fakenews.webcrawler

import java.awt.image.BufferedImage
import java.lang.RuntimeException
import org.opencv.calib3d.Calib3d
import org.opencv.calib3d.Calib3d.RANSAC
import org.opencv.core.*
import org.opencv.features2d.DescriptorMatcher
import org.opencv.features2d.KAZE
import org.opencv.imgcodecs.Imgcodecs
import org.opencv.imgproc.Imgproc


private var initialized: Boolean = false
fun initializeOpenCV() {
    if(!initialized) {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
        initialized = true
    }
}

fun Int.toByteArray(): ByteArray {
    return byteArrayOf(
        this.and(0x00ff0000).shr(16).toByte(),
        this.and(0x0000ff00).shr(8).toByte(),
        this.and(0x000000ff).toByte()
    )
}

fun BufferedImage.toMat(): Mat {
    val mat = Mat(this.height, this.width, CvType.CV_8UC3)
    for (i in (0 until this.height)){
        for (j in (0 until this.width)) {
            mat.put(i, j, this.getRGB(j, i).toByteArray())
        }
    }
    return mat
}


fun BufferedImage.ensureOpaque(): BufferedImage {
    if (this.transparency == BufferedImage.OPAQUE)
        return this
    val w = this.width
    val h = this.height
    val pixels = IntArray(w * h)
    this.getRGB(0, 0, w, h, pixels, 0, w)
    val bi2 = BufferedImage(w, h, BufferedImage.TYPE_INT_RGB)
    bi2.setRGB(0, 0, w, h, pixels, 0, w)
    return bi2
}

fun ByteArray.toMat(): Mat {
    val mat = Imgcodecs.imdecode(MatOfByte(*this), Imgcodecs.IMREAD_COLOR)
    Imgproc.cvtColor(mat, mat, Imgproc.COLOR_BGR2RGB)
    return mat
}

fun Mat.toBufferedImage(): BufferedImage {
    val w = this.width()
    val h = this.height()
    val res = BufferedImage(w, h, BufferedImage.TYPE_INT_RGB)
    for (i in (0 until w)) {
        for (j in (0 until h)) {
            res.setRGB(i, j, this[j, i].map { it.toInt() }.reduce { acc, v -> acc.shl(8).or(v) })
        }
    }
    return res
}

fun verticalStich(imageA: BufferedImage, imageB: BufferedImage,
                  lowerBarBound: Int=100, ratio: Double=0.75, reprojThresh: Double=4.0): BufferedImage{
    return verticalStich(imageA.toMat(), imageB.toMat(), lowerBarBound, ratio, reprojThresh).toBufferedImage()
}

fun verticalStich(imageA: Mat, imageB: Mat, lowerBarBound: Int=100, ratio: Double=0.75, reprojThresh: Double=4.0): Mat {
    val matA = imageB
    var matB = imageA
    matB = matB.submat(Rect(0, 0, matB.width(), matB.height() - lowerBarBound))

    val descA = detectFeatures(matA)
    val descB = detectFeatures(matB)

    // Match Keypoints
    val homography = matchKeypoints(descA, descB, ratio, reprojThresh)
    //End Matching

    val mappedPoints = (0 until matA.size().width.toInt()).flatMap {
        (0 until matA.size().height.toInt()).map { y -> it to y }
    }.map { calcDestPoint(it.first, it.second, homography) }
    val maxWidth = java.lang.Double.max(mappedPoints.map { it.x }.max()!!, matB.size().width)
    val maxHeight = java.lang.Double.max(mappedPoints.map { it.y }.max()!!, matB.size().height)
    val result = Mat()
    Imgproc.warpPerspective(matA, result, homography,
        Size(Point(maxWidth, maxHeight)))

    val bb= result.submat(0, matB.size().height.toInt(), 0, matB.size().width.toInt())
    matB.copyTo(bb)

    return result
}

private fun calcDestPoint(x: Int, y:Int, h: Mat): Point {
    val xd = (h[0,0][0]*x + h[0,1][0]*y + h[0,2][0]) / (h[2,0][0]*x + h[2,1][0]*y + h[2,2][0])
    val yd = (h[1,0][0]*x + h[1,1][0]*y + h[1,2][0]) / (h[2,0][0]*x + h[2,1][0]*y + h[2,2][0])
    return Point(xd, yd)
}

private fun matchKeypoints(
    descA: Pair<MatOfKeyPoint, Mat>,
    descB: Pair<MatOfKeyPoint, Mat>,
    ratio: Double,
    reprojThresh: Double
): Mat {
    val matches = mutableListOf<MatOfDMatch>()
    DescriptorMatcher.create(DescriptorMatcher.BRUTEFORCE).knnMatch(descA.second, descB.second, matches, 2)
    val matchPoints =
        matches.map { it.toArray() }.filter { it.size == 2 && it[0].distance < it[1].distance * ratio }.map { it[0] }
    if (matchPoints.size < 4) {
        throw RuntimeException()
    }
    val akpsA = descA.first.toArray()
    val ptsA = MatOfPoint2f(*matchPoints.map { akpsA[it.queryIdx].pt }.toTypedArray())
    val akpsB = descB.first.toArray()
    val ptsB = MatOfPoint2f(*matchPoints.map { akpsB[it.trainIdx].pt }.toTypedArray())
    return Calib3d.findHomography(ptsA, ptsB, RANSAC, reprojThresh)
}

private fun detectFeatures(image: Mat): Pair<MatOfKeyPoint, Mat> {
    val detector = KAZE.create()
    val descriptor = Mat()
    val keypoints = MatOfKeyPoint()
    detector.detectAndCompute(image, MatOfKeyPoint(), keypoints, descriptor)
    return keypoints to descriptor
}
