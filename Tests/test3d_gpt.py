# From ChatGPT with the following prompt:
# Write Python code using PySide6 to generate a 3D view of a grid of white lines on the XY axis, every 0.2 meters, where X = [-10 m 10m], and Y = [-10m 10m].

import sys
import PySide6.QtCore as QtCore # added
# from PySide6.QtCore import Qt
from PySide6.QtGui import QVector3D
import PySide6.QtWidgets as QtWidgets
import PySide6.Qt3DExtras as Qt3DExtras # added
# from PySide6.Qt3DExtras import Qt3DWindow, QEntity, QFirstPersonCameraController # removed
import PySide6.Qt3DRender as Qt3DRender # added
# from PySide6.Qt3DRender import QCamera, QCameraLens, QClearBuffers, QRenderAspect # removed
import PySide6.Qt3DCore as Qt3DCore # added
# from PySide6.Qt3DCore import QEntity, QTransform # removed
from PySide6.QtWidgets import QApplication
# from PySide6.Qt3DExtras import QSphereMesh
# from PySide6.Qt3DExtras import QPhongMaterial
# from PySide6.Qt3DExtras import QOrbitCameraController
# from PySide6.Qt3DExtras import QTorusMesh
# from PySide6.Qt3DRender import (QMesh, QColorMaterial)

def create_line_entity(start_pos, end_pos, parent_entity):
    line_entity = Qt3DCore.QEntity(parent_entity)
    
    # Create line mesh
    line_mesh = Qt3DRender.QMesh()
    # Set vertices of line mesh
    vertices = [
        start_pos.x(), start_pos.y(), start_pos.z(),
        end_pos.x(), end_pos.y(), end_pos.z()
    ]
    line_mesh.setVertexBuffer(Qt3DRender.QBuffer())
    line_mesh.vertexCount = len(vertices) // 3
    line_mesh.attributes.append(Qt3DRender.QAttribute(line_mesh.vertexCount, 3, line_mesh.vertexCount, 3, line_mesh.vertexCount * 12 , line_mesh))
    line_mesh.vertexCount = line_mesh.vertexCount
    line_mesh.attributes[0].vertexBuffer().setData(vertices)
    line_mesh.attributes[0].setName(Qt3DRender.QAttribute.defaultPositionAttributeName())
    
    # Create line material
    line_material = Qt3DRender.QColorMaterial(line_entity)
    line_material.setEffect(Qt3DRender.QColorMaterial.effect())
    line_material.setAmbient(Qt3DRender.QColorMaterial.createColor(QtCore.Qt.white))
    
    # Add mesh and material to line entity
    line_entity.addComponent(line_mesh)
    line_entity.addComponent(line_material)
    
    return line_entity

if __name__ == '__main__':
    # Create application
    app = QApplication(sys.argv)
    
    # Create window
    window = Qt3DExtras.Qt3DExtras.Qt3DWindow()
    container = QtWidgets.QWidget.createWindowContainer(window)
    container.setMinimumSize(800, 600)
    container.setWindowTitle("3D Grid")
    
    # Setup 3D scene
    scene = window.createDefaultWorld()
    camera = Qt3DRender.Qt3DCamera(scene)
    camera.lens().setPerspectiveProjection(45.0, 16/9, 0.1, 1000)
    camera.setPosition(QVector3D(0, 0, 20))
    camera.setViewCenter(QVector3D(0, 0, 0))
    
    # Add camera to scene
    scene.setActiveCamera(camera)
    
    # Create root entity
    root_entity = Qt3DCore.QEntity(scene)
    
    # Create grid lines
    line_length = 10
    line_spacing = 0.2
    num_lines = int(line_length / line_spacing)
    for i in range(num_lines + 1):
        x_start = -line_length / 2
        x_end = line_length / 2
        y = x_start + i * line_spacing
        start_pos = QVector3D(x_start, y, 0)
        end_pos = QVector3D(x_end, y, 0)
        # Create line entity
        line_entity = create_line_entity(start_pos, end_pos, root_entity)
        
        # Rotate line entity to align with XY plane
        transform = Qt3DCore.QTransform()
        angle = 90
        axis = QVector3D(1, 0, 0)
        transform.setRotation(Qt3DCore.QQuaternion.fromAxisAndAngle(axis, angle))
        line_entity.addComponent(transform)
        
        y_start = -line_length / 2
        y_end = line_length / 2
        x = y_start + i * line_spacing
        start_pos = QVector3D(x, y_start, 0)
        end_pos = QVector3D(x, y_end, 0)
        # Create line entity
        line_entity = create_line_entity(start_pos, end_pos, root_entity)
        
        # Add transform component to rotate line entity
        transform = Qt3DCore.QTransform()
        angle = 90
        axis = QVector3D(0, 1, 0)
        transform.setRotation(Qt3DCore.QQuaternion.fromAxisAndAngle(axis, angle))
        line_entity.addComponent(transform)
    
    # Create camera controller
    camera_controller = Qt3DExtras.QOrbitCameraController(root_entity)
    camera_controller.setCamera(camera)
    
    # Show window
    window.setContainer(container)
    window.show()
    
    # Run the event loop
    sys.exit(app.exec())