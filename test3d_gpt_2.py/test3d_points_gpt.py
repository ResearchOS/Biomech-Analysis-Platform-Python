from PySide6.QtCore import Qt
from PySide6.QtGui import QGuiApplication
from PySide6.Qt3DCore import Qt3DCore
from PySide6.Qt3DExtras import Qt3DExtras, Qt3DWindow, QPhongMaterial, QSphereMesh, QFirstPersonCameraController
from PySide6.Qt3DRender import QCamera, QPointLight, QRenderSettings

import random

# Create the application instance
app = QGuiApplication([])

# Create the Qt3D window
window = Qt3DExtras.Qt3DWindow()
window.defaultFrameGraph().setClearColor(Qt.white)
window.setTitle('3D Points')

# Create the entity and component objects
rootEntity = Qt3DCore.QEntity()
surfaceComponent = QPhongMaterial(rootEntity)
lightEntity = Qt3DCore.QEntity(rootEntity)
pointsEntity = Qt3DCore.QEntity(rootEntity)

# Create the camera
cameraEntity = window.camera()
cameraEntity.lens().setPerspectiveProjection(45.0, window.width() / window.height(), 0.1, 1000)
cameraEntity.setPosition(Qt.QVector3D(0, 0, 20.0))
cameraEntity.setViewCenter(Qt.QVector3D(0, 0, 0))

# Create the camera controller
camController = QFirstPersonCameraController(rootEntity)
camController.setCamera(cameraEntity)

# Create the light
lightEntity.addComponent(QPointLight(rootEntity))
lightTransform = Qt3DCore.QTransform(lightEntity)
lightTransform.setTranslation(Qt.QVector3D(10, 10, 10))
lightEntity.addComponent(lightTransform)

# Create the random 3D points
random.seed(0)
points = []
for _ in range(100):
    pointEntity = Qt3DCore.QEntity(pointsEntity)
    pointMesh = QSphereMesh()
    pointMesh.setRadius(0.1)
    pointEntity.addComponent(pointMesh)
    pointMaterial = QPhongMaterial(pointEntity)
    pointMaterial.setDiffuse(Qt.QColor(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
    pointEntity.addComponent(pointMaterial)
    pointTransform = Qt3DCore.QTransform(pointEntity)
    pointTransform.setTranslation(Qt.QVector3D(random.uniform(-5, 5), random.uniform(-5, 5), random.uniform(-5, 5)))
    pointEntity.addComponent(pointTransform)
    points.append(pointEntity)

# Set root entity
window.setRootEntity(rootEntity)

# Show the window
window.show()
window.resize(800, 600)

# Run the application event loop
app.exec()